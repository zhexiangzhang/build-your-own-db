using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using XXCore.Utilities;

namespace XXCore
{
    /**
    * 
    * For the db to be able to reuse space for deleted data instead of seeing the database file as a Stream, see data as individual blocks of equal size
    * 
    * note: firstSector is the first 4kb of the block, which may contain the header and part of the data, it serves as a cache (as the header may be read frequently)
    *       we only flush the firstSector to the stream when the block is disposed. The reason to use 4kb is that is also the size of OS file system to transfer data
    *           [However, it results in non-linear write for blocks larger than 4KB (as the first 4KB cached data always gets written last] If memory is not a concern, we can cache the whole block
    * 
    * note: we also use a header cache to cache the part of the header, so dont need to serialize the byte array every time
    * 
    * Important Functions:
    *      GetHeader(int field) : get the value of a header
    *      SetHeader(int field, long value) : set the value of a header
    *      Read(byte[] dst, int dstOffset, int srcOffset, int count) : read data of the block (src) into given buffer (dst)
    *      Write(byte[] src, int srcOffset, int dstOffset, int count) : write data from given buffer (src) into the block (dst)
    *      Dispose() : release the resources used by the block
    *      
    **/
    public class Block : IBlock
    {

        private readonly BlockStorage storage;
        private readonly Stream stream;
        private readonly byte[] firstSector;
        private readonly long?[] headerCache = new long?[Constant.headerCacheSize];

        private readonly uint id; // unique block id
        
        bool isFirstSectorDirty = false; 
        // if block is being released
        bool isDisposed = false;

        // Constructor
        public Block(BlockStorage storage, uint id, Stream stream, byte[] firstSector)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (firstSector == null)
                throw new ArgumentNullException("firstSector");

            if (storage.getDiskSectorSize() != firstSector.Length)
                throw new ArgumentException("firstSector size must be equal to disk sector size");
               
            this.storage = storage;
            this.id = id;
            this.stream = stream;
            this.firstSector = firstSector;   // DiskSectorSize
        }
        
        /**
         * public methods
         * **/

        public uint Id
        {
            get { return id; }
        }

        public long GetHeader(int field)
        {
            if (isDisposed)
                throw new ObjectDisposedException("Block");

            // because each header is 8 bytes
            if (field < 0 || field >= storage.getBlockHeaderSize()/8)
                throw new ArgumentOutOfRangeException("field");

            if (field < headerCache.Length)
            {
                if (headerCache[field] == null)
                {
                    headerCache[field] = BufferHelper.ReadBufferInt64(firstSector, field * 8);  
                }
                return headerCache[field].Value;
            }
            else
            {
                return BufferHelper.ReadBufferInt64(firstSector, field * 8);
            }
        }

        public void SetHeader(int field, long value)
        {
            if(isDisposed)
                throw new ObjectDisposedException("Block");
            if (field < 0 || field >= storage.getBlockHeaderSize() / 8)
                throw new ArgumentOutOfRangeException("field");

            // update the cache if the field is in the cache
            if (field < headerCache.Length)
            {
                headerCache[field] = value;
            }

            // write in header
            BufferHelper.WriteBuffer(value, firstSector, field * 8);
            isFirstSectorDirty = true;
        }

        public void Read(byte[] dst, int dstOffset, int srcOffset, int count)
        {
            if (isDisposed)
                throw new ObjectDisposedException("Block");

            // Validate argument
            if (false == ((count >= 0) && ((count + srcOffset) <= storage.getBlockDataSize())))
            {
                throw new ArgumentOutOfRangeException("Requested count is outside of src bounds: Count=" + count, "count");
            }

            if (false == ((count + dstOffset) <= dst.Length))
            {
                throw new ArgumentOutOfRangeException("Requested count is outside of dest bounds: Count=" + count);
            }

            var blockSize = storage.getBlockSize();
            var blockHeaderSize = storage.getBlockHeaderSize();
            var diskSectorSize = storage.getDiskSectorSize();

            // If part of remain data belongs to the firstSector buffer then copy from the firstSector first
            var dataCopied = 0;
            var copyFromFirstSector = (blockHeaderSize + srcOffset) < diskSectorSize;
            if (copyFromFirstSector)
            {
                var tobeCopied = Math.Min(diskSectorSize - blockHeaderSize - srcOffset, count);

                Buffer.BlockCopy(src: firstSector
                    , srcOffset: blockHeaderSize + srcOffset
                    , dst: dst
                    , dstOffset: dstOffset
                    , count: tobeCopied);

                dataCopied += tobeCopied;
            }

            // Move the stream to correct position,
            // if there is still some data tobe copied
            if (dataCopied < count)
            {
                if (copyFromFirstSector)
                {
                    stream.Position = (Id * blockSize) + diskSectorSize;
                }
                else
                {
                    stream.Position = (Id * blockSize) + blockHeaderSize + srcOffset;
                }
            }

            // Start copying until all data required is copied
            while (dataCopied < count)
            {
                var bytesToRead = Math.Min(diskSectorSize, count - dataCopied);
                var thisRead = stream.Read(dst, dstOffset + dataCopied, bytesToRead);
                if (thisRead == 0)
                {
                    throw new EndOfStreamException();
                }
                dataCopied += thisRead;
            }
        }

        public void Write(byte[] src, int srcOffset, int dstOffset, int count)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException("Block");
            }

            // Validate argument

            // dstOffset is the offset in the block, not in the stream, (should = 0 I think)
            if (false == ((dstOffset >= 0) && ((dstOffset + count) <= storage.getBlockDataSize())))
            {
                throw new ArgumentOutOfRangeException("Count is outside of dest bounds: Count=" + count, "count");
            }

            if (false == ((srcOffset >= 0) && ((srcOffset + count) <= src.Length)))
            {
                throw new ArgumentOutOfRangeException("Count is outside of src bounds: Count=" + count, "count");
            }

            // Write bytes that belong to the firstSector
            // Note: the data belongs to the first sector will only be flushed to the stream when the block is disposed
            // like a cache, the data is not written to the stream immediately
            if ((storage.getBlockHeaderSize() + dstOffset) < storage.getDiskSectorSize())
            {
                var bytesToCopy = Math.Min(count, storage.getDiskSectorSize() - (storage.getBlockHeaderSize() + dstOffset));
                Buffer.BlockCopy(src: src
                    , srcOffset: srcOffset
                    , dst: firstSector
                    , dstOffset: storage.getBlockHeaderSize() + dstOffset // leave space for header  [ [header]  *[part data] ], * is dstoffset
                    , count: bytesToCopy); 
                isFirstSectorDirty = true;
            }

            // Write bytes that belong to the rest of the block           
            // Note: headerSize + dstOffset + count > headerSize + dstOffset, the if branch means ! all the data can be written to the first sector
            if (storage.getBlockHeaderSize() + dstOffset + count > storage.getDiskSectorSize())
            {
                // Move underlying stream to correct position ready for writting
                var blockSize = storage.getBlockSize();
                var blockHeaderSize = storage.getBlockHeaderSize();
                var diskSectorSize = storage.getDiskSectorSize();
                stream.Position = (Id * blockSize) + Math.Max(diskSectorSize, blockHeaderSize + dstOffset);

                // Exclude bytes that have been written to the first sector
                var d = diskSectorSize - (blockHeaderSize + dstOffset);
                if (d > 0)
                {
                    dstOffset += d;
                    srcOffset += d;
                    count -= d;
                }

                // Keep writing until all data is written
                var written = 0;
                while (written < count)
                {
                    var bytesToWrite = (int)Math.Min(4096, count - written);
                    this.stream.Write(src, srcOffset + written, bytesToWrite);
                    this.stream.Flush();
                    written += bytesToWrite;
                }
            }

        }

        /**
         **  Dispose
         *   When a client finishes using a block, it must call Dispose for all the cache to be flushed.
         **/

        public event EventHandler Disposed;

        protected virtual void OnDisposed(EventArgs e)
        {
            if (Disposed != null)
            {
                Disposed(this, e);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this); // told the GC that the object has been disposed, so it doesn't need to call the finalizer again
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                isDisposed = true;

                if (isFirstSectorDirty)
                {
                    stream.Position = id * storage.getBlockSize();
                    stream.Write(firstSector, 0, firstSector.Length); // write the first sector to the stream
                    stream.Flush();  // force the stream to write the data to the disk
                    isFirstSectorDirty = false;
                }
            }

            OnDisposed(EventArgs.Empty);             
        }

        ~Block()
        {
            Dispose(false);
        }

    }   
}
