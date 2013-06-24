using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;
using System.IO;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestSeek
    {
        [TestMethod]
        public void TestWriteReadRandom_Seek1()
        {
            DurableFileStream dfs = new DurableFileStream("TestWriteReadRandom_Seek1.dat", true);

            long seekedPos = dfs.Seek(2000, SeekOrigin.Begin);
            Assert.AreEqual<long>(seekedPos, 2000);
            Assert.AreEqual<long>(dfs.Position, 2000);
            Assert.AreEqual<long>(dfs.Length, 0);

            seekedPos = dfs.Seek(-1000, SeekOrigin.Current);
            Assert.AreEqual<long>(seekedPos, 1000);
            Assert.AreEqual<long>(dfs.Position, 1000);
            Assert.AreEqual<long>(dfs.Length, 0);

            dfs.Close();
        }

        [TestMethod]
        public void TestWriteReadRandom_Seek2()
        {
            DurableFileStream dfs = new DurableFileStream("TestWriteReadRandom_Seek2.dat", true);

            // seek
            long seekedPos = dfs.Seek(2000, SeekOrigin.Begin);
            Assert.AreEqual<long>(seekedPos, 2000);
            Assert.AreEqual<long>(dfs.Position, 2000);
            Assert.AreEqual<long>(dfs.Length, 0);

            seekedPos = dfs.Seek(-1000, SeekOrigin.Current);
            Assert.AreEqual<long>(seekedPos, 1000);
            Assert.AreEqual<long>(dfs.Position, 1000);
            Assert.AreEqual<long>(dfs.Length, 0);

            // write
            int bufferSize = DurableFileStream.BLOCK_SIZE;
            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);
            dfs.Write(writeBuffer, 0, bufferSize);
            long lastPosition = seekedPos + bufferSize;
            Assert.AreEqual<long>(dfs.Position, lastPosition);
            Assert.AreEqual<long>(dfs.Length, lastPosition);

            seekedPos = dfs.Seek(-1000, SeekOrigin.End);
            Assert.AreEqual<long>(seekedPos, lastPosition - 1000);
            Assert.AreEqual<long>(dfs.Position, lastPosition - 1000);
            Assert.AreEqual<long>(dfs.Length, lastPosition);

            dfs.Commit();
            dfs.Close();
        }

        
    }
}
