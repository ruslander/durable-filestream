using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestCache
    {
        [TestMethod]
        public void TestCache1()
        {
            // Writer
            DurableFileStream dfs = new DurableFileStream("TestCache1.dat", true, 0);

            byte[] writeBuffer = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer);
            dfs.Write(writeBuffer, 0, writeBuffer.Length);

            byte[] writeBuffer2 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);

            byte[] writeBuffer3 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer3);
            dfs.Write(writeBuffer3, 0, writeBuffer2.Length);
            dfs.Commit();

            byte[] writeBuffer4 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer4);
            dfs.Write(writeBuffer4, 0, writeBuffer2.Length);

            dfs.Seek(-2*4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer = new byte[4096];
            dfs.Read(readBuffer, 0, readBuffer.Length);

            dfs.Seek(-2 * 4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer2 = new byte[4096];
            dfs.Read(readBuffer2, 0, readBuffer2.Length);

            dfs.Close();
        }

        [TestMethod]
        public void TestCache2()
        {
            // Writer
            DurableFileStream dfs = new DurableFileStream("TestCache2.dat", true, 5000);

            byte[] writeBuffer = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer);
            dfs.Write(writeBuffer, 0, writeBuffer.Length);

            byte[] writeBuffer2 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);

            byte[] writeBuffer3 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer3);
            dfs.Write(writeBuffer3, 0, writeBuffer2.Length);
            dfs.Commit();

            byte[] writeBuffer4 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer4);
            dfs.Write(writeBuffer4, 0, writeBuffer2.Length);

            dfs.Seek(-2 * 4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer = new byte[4096];
            dfs.Read(readBuffer, 0, readBuffer.Length);

            dfs.Seek(-2 * 4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer2 = new byte[4096];
            dfs.Read(readBuffer2, 0, readBuffer2.Length);

            dfs.Close();
        }

        [TestMethod]
        public void TestCache3()
        {
            // Writer
            DurableFileStream dfs = new DurableFileStream("TestCache3.dat", true, 8192);

            byte[] writeBuffer = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer);
            dfs.Write(writeBuffer, 0, writeBuffer.Length);

            byte[] writeBuffer2 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer2);
            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);

            byte[] writeBuffer3 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer3);
            dfs.Write(writeBuffer3, 0, writeBuffer2.Length);
            dfs.Commit();

            byte[] writeBuffer4 = new byte[4096];
            Helper.SetArrayRandomly(writeBuffer4);
            dfs.Write(writeBuffer4, 0, writeBuffer2.Length);

            dfs.Seek(-2 * 4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer = new byte[4096];
            dfs.Read(readBuffer, 0, readBuffer.Length);

            dfs.Seek(-2 * 4096, System.IO.SeekOrigin.Current);
            byte[] readBuffer2 = new byte[4096];
            dfs.Read(readBuffer2, 0, readBuffer2.Length);

            dfs.Close();
        }
    }
}
