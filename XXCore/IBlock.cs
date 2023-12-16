using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XXCore
{
    public interface IBlock : IDisposable // to release the resources used by the block
    {
        uint Id { get; } // unique block id

        // A block may contain multiple header metadata
        // each header identified by a field id
        long GetHeader(int field);

        // change the value of a header
        void SetHeader(int field, long value);

        // read data of the block (src) into given buffer (dst)
        void Read(byte[] dst, int dstOffset, int srcOffset, int count);

        // write data from given buffer (src) into the block (dst)
        void Write(byte[] src, int srcOffset, int dstOffset, int count);
    }
}
