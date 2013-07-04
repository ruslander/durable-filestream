using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DurableFile;

namespace UnitTestDurableFile
{
    [TestClass]
    public class UnitTestWriteRead
    {
        [TestMethod]
        public void TestWriteReadInt()
        {
            int x = 7812328;
            ExecuteWriteRead("TestWriteReadInt.dat", BitConverter.GetBytes(x));
        }

        [TestMethod]
        public void TestWriteReadBlock()
        {
            byte[] writeBuffer = new byte[DurableFileStream.BLOCK_SIZE];
            for (int i = 0; i < writeBuffer.Length; i += 2)
            {
                writeBuffer[i] = (byte)1;
                writeBuffer[i + 1] = (byte)0;
            }

            ExecuteWriteRead("TestWriteReadBlock.dat", writeBuffer);
        }

        [TestMethod]
        public void TestWriteReadRandom1()
        {
            int bufferSize = DurableFileStream.BLOCK_SIZE + DurableFileStream.BLOCK_SIZE / 2;
           
            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead("TestWriteReadRandom1.dat", writeBuffer);
        }

        [TestMethod]
        public void TestWriteReadRandom2()
        {
            int bufferSize = 31924; 
                
            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead("TestWriteReadRandom2.dat", writeBuffer);
        }

        [TestMethod]
        public void TestWriteReadRandom3()
        {
            Random rand = new Random();

            int bufferSize = rand.Next(2 * DurableFileStream.BLOCK_SIZE, 10 * DurableFileStream.BLOCK_SIZE);

            byte[] writeBuffer = new byte[bufferSize];
            Helper.SetArrayRandomly(writeBuffer);

            ExecuteWriteRead("TestWriteReadRandom3.dat", writeBuffer);
        }

        private void ExecuteWriteRead(string filename, byte[] writeBuffer)
        {
            int bufferSize = writeBuffer.Length;

            //  Writer
            DurableFileStream dfs = new DurableFileStream(filename, true);
            dfs.Write(writeBuffer, 0, bufferSize);
            dfs.Commit();
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
            Assert.IsTrue(Helper.EqualArray(readBuffer, writeBuffer));
        }

        private void SampleCode1()
        {
            //  Writer
            DurableFile.DurableFileStream dfs = new DurableFile.DurableFileStream("example1.dat", true);

            long data = 78739;
            // write to cache buffer only
            dfs.Write(BitConverter.GetBytes(data), 0, 8);

            data = 10254;
            // write to cache buffer only
            dfs.Write(BitConverter.GetBytes(data), 0, 8);

            data = 85471;
            // write to cache buffer only
            dfs.Write(BitConverter.GetBytes(data), 0, 8);

            // persist writes permanently to disk
            dfs.Commit();
            
            dfs.Close();

           
        }

        private void SampleCode2()
        {
            // Reader
            DurableFile.DurableFileStream dfs = new DurableFile.DurableFileStream("example1.dat", false);
            byte[] buffer = new byte[8];
            dfs.Read(buffer, 0, 8);
            long data = BitConverter.ToInt64(buffer, 0);
            Console.WriteLine(data);

            dfs.Read(buffer, 0, 8);
            data = BitConverter.ToInt64(buffer, 0);
            Console.WriteLine(data);

            dfs.Read(buffer, 0, 8);
            data = BitConverter.ToInt64(buffer, 0);
            Console.WriteLine(data);

            dfs.Close();
        }

    }
}
