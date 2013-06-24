using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestRecovery
    {
        [TestMethod]
        public void TestRecovery_AfterLogBegin()
        {
            Check_NO_UNDO("TestRecovery_AfterLogBegin.dat", DurableFileStream.FailedCommitCode.AFTER_LOG_BEGIN);
        }

        [TestMethod]
        public void TestRecovery_AfterLogWrite()
        {
            Check_NO_UNDO("TestRecovery_AfterLogWrite.dat", DurableFileStream.FailedCommitCode.AFTER_LOG_WRITE);
        }

        [TestMethod]
        public void TestRecovery_AfterLogCommit()
        {
            Check_REDO("TestRecovery_AfterLogCommit.dat", DurableFileStream.FailedCommitCode.AFTER_LOG_COMMIT);
        }

        [TestMethod]
        public void TestRecovery_AfterDiskWrite()
        {
            Check_REDO("TestRecovery_AfterDiskWrite.dat", DurableFileStream.FailedCommitCode.AFTER_DISK_WRITE);
        }

        [TestMethod]
        public void TestRecovery_AfterLogEnd()
        {
            Check_REDO("TestRecovery_AfterLogEnd.dat", DurableFileStream.FailedCommitCode.AFTER_LOG_END);
        }

        private void Check_NO_UNDO(string filename, DurableFileStream.FailedCommitCode failedCommitCode)
        {
            int bufferSize = 10005;

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            //  Writer
            try
            {
                DurableFileStream dfs = new DurableFileStream(filename, true);
                dfs.Write(writeBuffer, 0, bufferSize);
                dfs.__FailedCommitSimulation__(failedCommitCode);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                DurableFileStream dfs = new DurableFileStream(filename, false);
                Assert.AreEqual<long>(dfs.Position, 0);
                Assert.AreEqual<long>(dfs.Length, 0);
                dfs.Write(writeBuffer, 0, bufferSize);
                dfs.Commit();

                Assert.AreEqual<long>(dfs.Position, bufferSize);
                Assert.AreEqual<long>(dfs.Length, bufferSize);

                long x = 9876543210;
                dfs.Write(BitConverter.GetBytes(x), 0, 8);
                dfs.Commit();

                Assert.AreEqual<long>(dfs.Position, bufferSize + 8);
                Assert.AreEqual<long>(dfs.Length, bufferSize + 8);

                dfs.Close();

                dfs = new DurableFileStream(filename, false);
                byte[] readBuffer = new byte[bufferSize];
                dfs.Read(readBuffer, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(writeBuffer, readBuffer));
                dfs.Read(readBuffer, 0, 8);
                long y = BitConverter.ToInt64(readBuffer, 0);
                Assert.AreEqual<long>(x, y);

                dfs.Close();
            }
        }

        private void Check_REDO(string filename, DurableFileStream.FailedCommitCode failedCommitCode)
        {
            int bufferSize = 10005;
            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            try
            {
                //  Writer
                DurableFileStream dfs = new DurableFileStream(filename, true);
                dfs.Write(writeBuffer, 0, bufferSize);
                dfs.__FailedCommitSimulation__(failedCommitCode);
                Assert.Fail("should not pass here!");
                dfs.Close();
            }
            catch (Exception)
            {
                // reader
                DurableFileStream dfs = new DurableFileStream(filename, false);
                Assert.AreEqual<long>(dfs.Position, 0);
                Assert.AreEqual<long>(dfs.Length, bufferSize);

                byte[] readBuffer = new byte[bufferSize];
                dfs.Read(readBuffer, 0, bufferSize);
                Assert.IsTrue(Helper.EqualArray(writeBuffer, readBuffer));

                Assert.AreEqual<long>(dfs.Position, bufferSize);
                Assert.AreEqual<long>(dfs.Length, bufferSize);

                dfs.Close();
            }
        }
    }
}
