using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTest
{
    public static class UnitTestHelper
    {
        public static byte[] RandomData(int length)
        {
            var data = new byte[length];
            var rnd = new Random();
            for (var i = 0; i < data.Length; i++)
            {
                data[i] = (byte)rnd.Next(0, 256);
            }
            return data;
        }
    }
}