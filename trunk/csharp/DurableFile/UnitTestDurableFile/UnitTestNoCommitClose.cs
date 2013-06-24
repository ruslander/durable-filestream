using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestNoCommitClose
    {
        [TestMethod]
        public void TestWriteReadRandom_NoCommit_Close()
        {
            Random rand = new Random();

            int bufferSize = rand.Next(2 * DurableFileStream.BLOCK_SIZE, 10 * DurableFileStream.BLOCK_SIZE);

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead_NoCommit_Close("TestWriteReadRandom_NoCommit_Close.dat", writeBuffer);
        }

        private void ExecuteWriteRead_NoCommit_Close(string filename, byte[] writeBuffer)
        {
            int bufferSize = writeBuffer.Length;

            //  Writer
            DurableFileStream dfs = new DurableFileStream(filename, true);
            dfs.Write(writeBuffer, 0, bufferSize);
            Assert.AreEqual<long>(dfs.Position, bufferSize);
            Assert.AreEqual<long>(dfs.Length, bufferSize);
            dfs.Close();

            // Reader
            dfs = new DurableFileStream(filename, false);
            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.AreEqual<long>(dfs.Position, bufferSize);
            Assert.AreEqual<long>(dfs.Length, bufferSize);
            dfs.Close();

            // Validate
            Assert.IsTrue(Helper.EqualArray(writeBuffer, readBuffer));
        }
    }
}
