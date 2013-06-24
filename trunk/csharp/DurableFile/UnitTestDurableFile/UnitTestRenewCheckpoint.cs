using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestRenewCheckpoint
    {
        [TestMethod]
        public void TestRenewCheckpoint1()
        {
            // RENEW_CHECKPOINT_THRESHOLD = 10 * 1024;
            int bufferSize = 20000;

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint1.dat", true);
            dfs.Write(writeBuffer, 0, bufferSize);
            dfs.Commit();
            dfs.Close();

            // reader
            dfs = new DurableFileStream("TestRenewCheckpoint1.dat", false);
            byte[] readBuffer = new byte[bufferSize];
            dfs.Read(readBuffer, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer));
            dfs.Close();
        }

        [TestMethod]
        public void TestRenewCheckpoint2()
        {
            // RENEW_CHECKPOINT_THRESHOLD = 10 * 1024;
            int bufferSize = 20000;
            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[1000];
            Helper.SetArrayRandomly(writeBuffer2);

            // writer
            DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint2.dat", true);
            dfs.Write(writeBuffer1, 0, bufferSize);
            dfs.Commit();

            dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
            dfs.Commit();

            dfs.Close();

            // reader
            dfs = new DurableFileStream("TestRenewCheckpoint2.dat", false);
            byte[] readBuffer1 = new byte[bufferSize];
            dfs.Read(readBuffer1, 0, bufferSize);
            Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));

            byte[] readBuffer2 = new byte[1000];
            dfs.Read(readBuffer2, 0, readBuffer2.Length);
            Assert.IsTrue(Helper.EqualArray(readBuffer2, writeBuffer2));

            dfs.Close();
        }

        [TestMethod]
        public void TestRenewCheckpoint_AfterLogBegin1()
        {
            int bufferSize = 20000;

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            try
            {
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_AfterLogBegin1.dat", true);
                dfs.Write(writeBuffer, 0, bufferSize);
                dfs.__FailedCommitSimulation__(DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_BEGIN);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_AfterLogBegin1.dat", false);
                byte[] readBuffer = new byte[bufferSize];
                dfs.Read(readBuffer, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer));
                dfs.Close();
            }
        }

        [TestMethod]
        public void TestRenewCheckpoint_AfterLogBegin2()
        {
            int bufferSize = 20000;

            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);

            try
            {
                // writer
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_AfterLogBegin2.dat", true);
                dfs.Write(writeBuffer1, 0, bufferSize);
                dfs.Commit();
                
                dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
                dfs.__FailedCommitSimulation__(DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_BEGIN);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_AfterLogBegin2.dat", false);

                byte[] readBuffer1 = new byte[bufferSize];
                dfs.Read(readBuffer1, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));

                byte[] readBuffer2 = new byte[writeBuffer2.Length];
                dfs.Read(readBuffer2, 0, readBuffer2.Length);

                dfs.Close();
            }
        }

        [TestMethod]
        public void TestRenewCheckpoint_AfterLogWrite()
        {
            int bufferSize = 20000;

            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);

            long checkpointAdr = 0;

            try
            {
                // writer
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_3.dat", true);
                dfs.Write(writeBuffer1, 0, bufferSize);
                dfs.Commit();

                dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
                dfs.__FailedCommitSimulation__(DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_WRITE);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_3.dat", false);

                byte[] readBuffer1 = new byte[bufferSize];
                dfs.Read(readBuffer1, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));

                byte[] readBuffer2 = new byte[writeBuffer2.Length];
                dfs.Read(readBuffer2, 0, readBuffer2.Length);

                dfs.Close();
            }
        }

        [TestMethod]
        public void TestRenewCheckpoint_AfterDiskWrite()
        {
            int bufferSize = 20000;

            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);

            long checkpointAdr = 0;

            try
            {
                // writer
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_4.dat", true);
                dfs.Write(writeBuffer1, 0, bufferSize);
                dfs.Commit();

                dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
                dfs.__FailedCommitSimulation__(DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_DISK_WRITE);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_4.dat", false);

                byte[] readBuffer1 = new byte[bufferSize];
                dfs.Read(readBuffer1, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));

                byte[] readBuffer2 = new byte[writeBuffer2.Length];
                dfs.Read(readBuffer2, 0, readBuffer2.Length);

                dfs.Close();
            }
        }

        [TestMethod]
        public void TestRenewCheckpoint_AfterLogEnd()
        {
            int bufferSize = 20000;

            byte[] writeBuffer1 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer1);

            byte[] writeBuffer2 = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer2);

            long checkpointAdr = 0;

            try
            {
                // writer
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_5.dat", true);
                dfs.Write(writeBuffer1, 0, bufferSize);
                dfs.Commit();

                dfs.Write(writeBuffer2, 0, writeBuffer2.Length);
                dfs.__FailedCommitSimulation__(DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_END);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream("TestRenewCheckpoint_5.dat", false);

                byte[] readBuffer1 = new byte[bufferSize];
                dfs.Read(readBuffer1, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(readBuffer1, writeBuffer1));

                byte[] readBuffer2 = new byte[writeBuffer2.Length];
                dfs.Read(readBuffer2, 0, readBuffer2.Length);

                dfs.Close();
            }
        }


    }
}
