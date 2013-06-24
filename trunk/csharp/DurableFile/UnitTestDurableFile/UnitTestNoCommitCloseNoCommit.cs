using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestNoCommitCloseNoCommit
    {
        [TestMethod]
        public void TestWriteReadRandom_NoCommit_CloseNoCommit()
        {
            Random rand = new Random();

            int bufferSize = rand.Next(2 * DurableFileStream.BLOCK_SIZE, 10 * DurableFileStream.BLOCK_SIZE);

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead_NoCommit_CloseNoCommit("TestWriteReadRandom_NoCommit_CloseNoCommit.dat", writeBuffer);
        }

        private void ExecuteWriteRead_NoCommit_CloseNoCommit(string filename, byte[] writeBuffer)
        {
            int bufferSize = writeBuffer.Length;

            //  Writer
            DurableFileStream dfs = new DurableFileStream(filename, true);
            dfs.Write(writeBuffer, 0, bufferSize);
            Assert.AreEqual<long>(dfs.Position, bufferSize);
            Assert.AreEqual<long>(dfs.Length, bufferSize);
            dfs.Close(false);

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
