using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestRecreateLogFile
    {
        [TestMethod]
        public void TestRecreateLogFile1()
        {
            byte[] writeBuffer = new byte[4096];

            //  Writer
            DurableFileStream dfs = new DurableFileStream("TestRecreateLogFile1.dat", true);

            for (int i = 0; i < /*13000*/100; i++)
            {
                Helper.SetArrayRandomly(writeBuffer);
                dfs.Write(writeBuffer, 0, writeBuffer.Length);
            }
            dfs.Commit();

            dfs.Close();
        }
    }
}
