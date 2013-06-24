using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestAbort
    {
        [TestMethod]
        public void TestWriteReadRandom_Abort1()
        {
            Random rand = new Random();

            int bufferSize = rand.Next(2 * DurableFileStream.BLOCK_SIZE, 10 * DurableFileStream.BLOCK_SIZE);

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead_Abort("TestWriteReadRandom_Abort1.dat", writeBuffer);
        }

        [TestMethod]
        public void TestWriteReadRandom_Abort2()
        {
            int bufferSize = 32914;

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead_Abort("TestWriteReadRandom_Abort2.dat", writeBuffer);
        }

        private void ExecuteWriteRead_Abort(string filename, byte[] writeBuffer)
        {
            int bufferSize = writeBuffer.Length;

            //  Writer
            DurableFileStream dfs = new DurableFileStream(filename, true);
            dfs.Write(writeBuffer, 0, bufferSize);
            Assert.AreEqual<long>(dfs.Position, bufferSize);
            Assert.AreEqual<long>(dfs.Length, bufferSize);
            dfs.Abort();
            dfs.Close();

            // Reader
            dfs = new DurableFileStream(filename, false);
            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.AreEqual<long>(dfs.Position, 0);
            Assert.AreEqual<long>(dfs.Length, 0);
            dfs.Close();

            // Validate
            Assert.IsTrue(!Helper.EqualArray(writeBuffer, readBuffer));
        }
    }
}
