using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace XXCore
{
    public interface IBlockStorage
    {
        // total size (byte) of a block = header + data
        int getBlockSize();

        // size of the header
        int getBlockHeaderSize();

        // size of the data
        int getBlockDataSize();
       
        // find a block by id
        IBlock Find(uint blockId);

        // Allocate a new block
        IBlock CreateNew();
    }
}
