using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UnitTestDurableFile
{
    class Helper
    {
        public static void SetArrayRandomly(byte[] array)
        {
            Random rand = new Random((int)System.Diagnostics.Stopwatch.GetTimestamp());

            for (int i = 0; i < array.Length; i++)
            {
                array[i] = (byte)rand.Next(0, 2);
            }
        }

        public static bool EqualArray(byte[] arrayA, byte[] arrayB)
        {
            if (arrayA.Length != arrayB.Length)
                return false;
            
            for (int i = 0; i < arrayA.Length; i++)
                if (arrayA[i] != arrayB[i])
                    return false;

            return true;
        }

        public static bool IsArrayEmpty(byte[] array)
        {
            for (int i = 0; i < array.Length; i++)
                if (array[i] != 0)
                    return false;

            return true;
        }
    }
}
