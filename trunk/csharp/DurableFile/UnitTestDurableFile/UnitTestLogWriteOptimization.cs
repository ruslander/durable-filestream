using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestLogWriteOptimization
    {
        [TestMethod]
        public void TestLogWriteOptimization1()
        {
            Random rand = new Random();

            byte[] writeBuffer1 = new byte[1000];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[1500];
            Helper.SetArrayRandomly(writeBuffer2);

            //  Writer
            DurableFileStream dfs = new DurableFileStream("TestLogWriteOptimization1.dat", true);
            dfs.Write(writeBuffer1, 0, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length);

            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length + writeBuffer2.Length);
            dfs.Commit();

            dfs.Close();

            // Reader
            dfs = new DurableFileStream("TestLogWriteOptimization1.dat", false);
            byte[] readBuffer1 = new byte[writeBuffer1.Length];
            dfs.Read(readBuffer1, 0, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length + writeBuffer2.Length);

            byte[] readBuffer2 = new byte[writeBuffer2.Length];
            dfs.Read(readBuffer2, 0, writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length + writeBuffer2.Length);

            dfs.Close();

            // Validate
            Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));
            Assert.IsTrue(Helper.EqualArray(readBuffer2, writeBuffer2));
        }

        [TestMethod]
        public void TestLogWriteOptimization2()
        {
            Random rand = new Random();

            byte[] writeBuffer1 = new byte[2500];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[1000];
            Helper.SetArrayRandomly(writeBuffer2);

            //
            //  Writer
            //
            DurableFileStream dfs = new DurableFileStream("TestLogWriteOptimization2.dat", true);
            dfs.Write(writeBuffer1, 0, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length);

            dfs.Seek(500, System.IO.SeekOrigin.Begin);
            Assert.AreEqual<long>(dfs.Position, 500);

            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Position, 500 + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length);
            dfs.Commit();

            dfs.Close();

            //
            // Reader
            //
            dfs = new DurableFileStream("TestLogWriteOptimization2.dat", false);
            byte[] readBuffer = new byte[writeBuffer1.Length];
            dfs.Read(readBuffer, 0, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length);

            byte[] mergedWriteBuffer = new byte[writeBuffer1.Length];
            Array.Copy(writeBuffer1, 0, mergedWriteBuffer, 0, writeBuffer1.Length);
            Array.Copy(writeBuffer2, 0, mergedWriteBuffer, 500, writeBuffer2.Length);

            dfs.Close();

            // Validate
            Assert.IsTrue(Helper.EqualArray(readBuffer, mergedWriteBuffer));
        }

        [TestMethod]
        public void TestLogWriteOptimization3()
        {
            Random rand = new Random();

            byte[] writeBuffer1 = new byte[2500];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[3000];
            Helper.SetArrayRandomly(writeBuffer2);

            //
            //  Writer
            //
            DurableFileStream dfs = new DurableFileStream("TestLogWriteOptimization2.dat", true);
            dfs.Write(writeBuffer1, 0, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Position, writeBuffer1.Length);
            Assert.AreEqual<long>(dfs.Length, writeBuffer1.Length);

            dfs.Seek(1000, System.IO.SeekOrigin.Begin);
            Assert.AreEqual<long>(dfs.Position, 1000);

            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Position, 1000 + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Length, 1000 + writeBuffer2.Length);
            dfs.Commit();

            dfs.Close();

            //
            // Reader
            //
            dfs = new DurableFileStream("TestLogWriteOptimization2.dat", false);
            byte[] readBuffer = new byte[1000 + writeBuffer2.Length];
            dfs.Read(readBuffer, 0, 1000 + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Position, 1000 + writeBuffer2.Length);
            Assert.AreEqual<long>(dfs.Length, 1000 + writeBuffer2.Length);

            byte[] mergedWriteBuffer = new byte[1000 + writeBuffer2.Length];
            Array.Copy(writeBuffer1, 0, mergedWriteBuffer, 0, writeBuffer1.Length);
            Array.Copy(writeBuffer2, 0, mergedWriteBuffer, 1000, writeBuffer2.Length);

            dfs.Close();

            // Validate
            Assert.IsTrue(Helper.EqualArray(readBuffer, mergedWriteBuffer));
        }
    }
}
