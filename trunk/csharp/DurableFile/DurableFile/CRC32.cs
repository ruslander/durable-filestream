using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DurableFile
{
    class CRC32
    {
        public const uint POLYNOMIAL = 0x82f63b78;

        static UInt32[] _table;

        static CRC32()
        {
            CreateTable();
        }

        private static void CreateTable()
        {
            _table = new uint[256];

            for (int i = 0; i < _table.Length; i++)
            {
                UInt32 crc32 = (UInt32)i;

                for (int j = 0; j < 8; j++)
                {
                    if ((crc32 & 1) == 1)
                        crc32 = (crc32 >> 1) ^ POLYNOMIAL;
                    else
                        crc32 >>= 1;
                }

                _table[i] = crc32;
            }
        }

        public static UInt32 Compute(byte[] message)
        {
            return Compute(message, 0, message.Length);
        }

        public static UInt32 Compute(byte[] message, int offset, int length)
        {
            UInt32 crc32 = 0;
            for (int i = offset; i < length; i++)
            {
                unchecked
                {
                    crc32 = _table[(crc32 ^ message[i]) & 0xff] ^ (crc32 >> 8);
                }
            }
            return crc32;
        }
    }
}
