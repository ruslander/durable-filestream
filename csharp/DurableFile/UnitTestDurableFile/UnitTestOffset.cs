using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestOffset
    {
        [TestMethod]
        public void TestOffset1()
        {
            int bufferSize = 4096;

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            //
            //  Writer
            //
            DurableFileStream dfs = new DurableFileStream("TestOffset1.dat", true);
            int offset = 100;
            int count = 500;
            dfs.Write(writeBuffer, offset, count);

            dfs.Seek(0, System.IO.SeekOrigin.Begin);
            byte[] readBuffer = new byte[count * 2];
            int readOffest = 300;
            dfs.Read(readBuffer, readOffest, count);
            bool equal = true;
            for (int i = offset; i < count; i++)
            {
                if (writeBuffer[i] != readBuffer[i - offset + readOffest])
                {
                    equal = false;
                    break;
                }
            }
            Assert.IsTrue(equal);

            dfs.Commit();
            Assert.AreEqual<long>(dfs.Position, count);
            Assert.AreEqual<long>(dfs.Length, count);

            dfs.Seek(0, System.IO.SeekOrigin.Begin);
            readBuffer = new byte[count * 2];
            dfs.Read(readBuffer, readOffest, count);
            equal = true;
            for (int i = offset; i < count; i++)
            {
                if (writeBuffer[i] != readBuffer[i - offset + readOffest])
                {
                    equal = false;
                    break;
                }
            }
            Assert.IsTrue(equal);

            dfs.Close();

            //
            // Reader
            //
            dfs = new DurableFileStream("TestOffset1.dat", false);
            Assert.AreEqual<long>(dfs.Position, 0);
            Assert.AreEqual<long>(dfs.Length, count);
            int count2 = count - 10;
            readBuffer = new byte[count2 * 3];
            readOffest = 750;
            dfs.Read(readBuffer, readOffest, count2);

            equal = true;
            for (int i = offset; i < count2; i++)
            {
                if (writeBuffer[i] != readBuffer[i - offset + readOffest])
                {
                    equal = false;
                    break;
                }
            }
            Assert.IsTrue(equal);

            Assert.AreEqual<long>(dfs.Position, count2);
            Assert.AreEqual<long>(dfs.Length, count);

            dfs.Close();
        }
    }
}