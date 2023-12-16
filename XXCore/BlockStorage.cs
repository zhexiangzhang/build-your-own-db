using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XXCore
{
    /**
     * 
     * The block storage is responsible for managing all blocks, which provide interface to create and find blocks
     * Important Functions:
     *      Find(uint blockId) : find a block by id
     *      Create() : Allocate a new block
     **/
    internal class BlockStorage : IBlockStorage
    {
        private readonly Stream stream;
        private readonly int blockSize;
        private readonly int blockHeaderSize;
        private readonly int blockDataSize;
        private readonly int unitOfWork;
        private readonly Dictionary<uint, Block> blocks = new Dictionary<uint, Block>();

        // Constructor
        // receive a stream, which can be a file stream / memory stream / network stream ...
        // Modern file systems transfer data in 4KB blocks. To make sure db is aligned with the underlying file system block size,
        // we can use 256B, 512B, 1024B, etc.. , here we use 4KB = 4096B
        public BlockStorage(Stream storage, int blockSize = 4096, int blockHeaderSize = 48)
        {
            if (storage == null)
                throw new ArgumentNullException("storage");

            if (blockHeaderSize > blockSize){
                throw new ArgumentException("blockHeaderSize must be less than blockSize");
            }

            if (blockSize < 128){
                throw new ArgumentException("blockSize must be greater than 128");
            }

            this.stream = storage;
            this.blockSize = blockSize;
            this.blockHeaderSize = blockHeaderSize;
            this.blockDataSize = blockSize - blockHeaderSize;
            // 4096 (4KB) is the disk sector size
            this.unitOfWork = ((blockSize >= 4096) ? 4096 : 128);
        }

        /**
         *   Public Methods
         **/

        public IBlock Create()
        {
            if ((this.stream.Length % blockSize) != 0)
            {
                throw new DataMisalignedException("Unexpected length of the stream: " + this.stream.Length);
            }

            // Calculate new block id
            var blockId = (uint)Math.Ceiling( (double)stream.Length / (double)blockSize);

            // Extend length of underlying stream
            this.stream.SetLength((long)((blockId * blockSize) + blockSize));
            this.stream.Flush();

            // Return desired block
                // the last parameter : firstSector is a cache for the first 4KB of the block (the size of physical disk sector size)
                // "first Sector that stores the block, may be header or combination of header and data"
            var block = new Block(this, blockId, stream, new byte[getDiskSectorSize()]); 
            blocks.Add(blockId, block);

            // when block disposed, remove it from memory
            block.Disposed += HandleBlockDisposed;
            return block;
        }

        public IBlock Find(uint blockId)
        {
            if (blocks.ContainsKey(blockId))
            {
                return blocks[blockId];
            }

            // move to the block position
            // if there is no block at the position, return null
            var blockPosition = blockId * blockSize;
            if (blockPosition + blockSize > stream.Length)
            {
                return null;
            }

            // Read the first 4KB of the block to construct a block from it
            var firstSector = new byte[getDiskSectorSize()];
            stream.Position = blockId * blockSize;
            stream.Read(firstSector, 0, getDiskSectorSize());

            var block = new Block(this, blockId, stream, firstSector);
            blocks.Add(blockId, block);

            // when block disposed, remove it from memory
            block.Disposed += HandleBlockDisposed;
            return block;
        }

        private void Block_Disposed(object sender, EventArgs e)
        {
            throw new NotImplementedException();
        }

        public int getDiskSectorSize()
        {
            return unitOfWork;
        }

        public int getBlockDataSize()
        {
            return blockDataSize;
        }

        public int getBlockHeaderSize()
        {
            return blockHeaderSize;
        }

        public int getBlockSize()
        {
            return blockSize;
        }

        /**
         **  Protect Methods
         **/
        protected virtual void HandleBlockDisposed(object sender, EventArgs e)
        {
            // Stop listening to it
            var block = (Block)sender;
            block.Disposed -= HandleBlockDisposed;

            // Remove it from memory
            blocks.Remove(block.Id);
        }
    }
}
