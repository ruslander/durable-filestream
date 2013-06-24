using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestMisc
    {
        /// <summary>
        /// Write | Abort | Write | Commit
        /// </summary>
        [TestMethod]
        public void TestMisc1()
        {
            //
            // write
            //
            int bufferSize = DurableFileStream.BLOCK_SIZE;
            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);
            DurableFileStream dfs = new DurableFileStream("TestMisc1.dat", true);
            dfs.Write(writeBuffer, 0, bufferSize);

            dfs.Abort();

            Helper.SetArrayRandomly(writeBuffer);
            dfs.Write(writeBuffer, 0, bufferSize);
            dfs.Commit();
            dfs.Close();

            //
            // read
            //
            dfs = new DurableFileStream("TestMisc1.dat", false);
            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.IsArrayEmpty(readBuffer));

            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer));
            dfs.Close();
        }

        /// <summary>
        /// Write | Commit | Write | Abort
        /// </summary>
        [TestMethod]
        public void TestMisc2()
        {
            //
            // write
            //
            int bufferSize = DurableFileStream.BLOCK_SIZE;
            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);
            DurableFileStream dfs = new DurableFileStream("TestMisc2.dat", true);
            dfs.Write(writeBuffer1, 0, bufferSize);
            dfs.Commit();

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, bufferSize);

            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Abort();

            Assert.AreEqual<long>(dfs.Length, 1 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Close();

            //
            // read
            //
            dfs = new DurableFileStream("TestMisc2.dat", false);
            Assert.AreEqual<long>(dfs.Length, DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 0);

            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer1));

            Assert.AreEqual<long>(dfs.Length, DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, DurableFileStream.BLOCK_SIZE);

            dfs.Close();
        }

        /// <summary>
        /// Write | Commit | Write | Commit
        /// </summary>
        [TestMethod]
        public void TestMisc3()
        {
            //
            // write
            //
            int bufferSize = DurableFileStream.BLOCK_SIZE;
            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);
            DurableFileStream dfs = new DurableFileStream("TestMisc3.dat", true);
            dfs.Write(writeBuffer1, 0, bufferSize);
            dfs.Commit();

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, bufferSize);

            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Commit();

            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Close();

            //
            // read
            //
            dfs = new DurableFileStream("TestMisc3.dat", false);
            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 0);

            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer1));

            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer2));

            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Close();
        }

        /// <summary>
        /// Write | Abort | Write | Abort
        /// </summary>
        [TestMethod]
        public void TestMisc4()
        {
            //
            // write
            //
            int bufferSize = DurableFileStream.BLOCK_SIZE;
            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);
            DurableFileStream dfs = new DurableFileStream("TestMisc4.dat", true);
            dfs.Write(writeBuffer1, 0, bufferSize);
            dfs.Abort();

            Assert.AreEqual<long>(dfs.Length, 0);
            Assert.AreEqual<long>(dfs.Position, DurableFileStream.BLOCK_SIZE);

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, bufferSize);

            Assert.AreEqual<long>(dfs.Length, 2 * DurableFileStream.BLOCK_SIZE);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Abort();

            Assert.AreEqual<long>(dfs.Length, 0);
            Assert.AreEqual<long>(dfs.Position, 2 * DurableFileStream.BLOCK_SIZE);

            dfs.Close();

            //
            // read
            //
            dfs = new DurableFileStream("TestMisc4.dat", false);
            Assert.AreEqual<long>(dfs.Length, 0);
            Assert.AreEqual<long>(dfs.Position, 0);

            byte[] readBuffer = new byte[bufferSize];
            int n = dfs.Read(readBuffer, 0, bufferSize);
            Assert.AreEqual<int>(n, 0);

            dfs.Close();
        }
    }
}
