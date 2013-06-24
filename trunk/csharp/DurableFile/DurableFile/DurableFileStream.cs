/* Copyright (c) 2013 Johnny Azzi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace DurableFile
{
    /// <summary>
    /// Durability (wiki): durability is the ACID property which guarentees that transactions that
    /// have been committed will survive permanently.
    /// 
    /// Recovery technique: Deferred Update (NO-UNDO/REDO recovery algorithm)
    /// </summary>
    public class DurableFileStream
    {
        /// <summary>
        /// Internal Block Size in bytes
        /// </summary>
        public const int BLOCK_SIZE = 4096; // 4K

        /// <summary>
        /// In bytes
        /// </summary>
        public const int DEFAULT_CACHE_SIZE = 256 * BLOCK_SIZE; // 1MB

        protected FileStream _fs;

        protected string _path;

        protected long _position;

        protected long _length;

        internal BlockCacheLRU _cacheBlocksLRU;

        private CommitLog _commitLog;

        /// <summary>
        ///  Initializes a new instance of the <code>DurableFile.DurableFileStream</code> class with the specified 
        ///  path and creation mode.
        /// </summary>
        /// <param name="path">A relative or absolute path for the file that the current RobustFileStream object will encapsulate.</param>
        /// <param name="create">Specifies that the operating system should create a new file. If the file already exists, it will be overwritten.</param>
        /// <param name="cacheSize">Specifies cache size in bytes. Minimum cache size is one block (4096bytes) and it should a multiple of 4096</param>
        public DurableFileStream(string path, bool create, int cacheSize)
        {
            ConstructorCode(path, create, cacheSize);
        }

        /// <summary>
        ///  Initializes a new instance of the <code>DurableFile.DurableFileStream</code> class with the specified 
        ///  path and creation mode.
        /// </summary>
        /// <param name="path">A relative or absolute path for the file that the current RobustFileStream object will encapsulate.</param>
        /// <param name="create">Specifies that the operating system should create a new file. If the file already exists, it will be overwritten.</param>
        public DurableFileStream(string path, bool create)
        {
            ConstructorCode(path, create, DEFAULT_CACHE_SIZE);
        }

        private void ConstructorCode(string path, bool create, int cacheSize)
        {
            _path = path;

            //
            // Create Cache Block
            // Convert cacheSize(in Bytes) to cacheBlockCount(in BLOCK_SIZE)
            //
            int cacheBlockCount = 1;
            if (cacheSize > BLOCK_SIZE)
            {
                cacheBlockCount = cacheSize / BLOCK_SIZE;
                if (cacheSize % BLOCK_SIZE != 0)
                    cacheBlockCount++;
            }
            _cacheBlocksLRU = new BlockCacheLRU(cacheBlockCount);

            //
            // Create or Open File
            //
            FileMode fileMode = FileMode.OpenOrCreate;
            if (create)
                fileMode = FileMode.Create;

            _fs = new FileStream(path, fileMode, FileAccess.ReadWrite, FileShare.ReadWrite, 16 * BLOCK_SIZE, FileOptions.WriteThrough);

            _commitLog = new CommitLog(this, create);

            if (_fs.Position > 0)
                _fs.Seek(0, SeekOrigin.Begin);

            _position = _fs.Position;
            _length = _fs.Length;
        }

        /// <summary>
        /// Gets the relative or absolute path for the file that the current FileStream object will encapsulate.
        /// </summary>
        public string Path
        {
            get
            {
                return _path;
            }
        }

        /// <summary>
        /// Gets the FileStream.
        /// </summary>
        public FileStream FileStream
        {
            get
            {
                return _fs;
            }
        }

        /// <summary>
        /// Gets the current position of this stream.
        /// </summary>
        public long Position
        {
            get
            {
                return _position;
            }
        }

        /// <summary>
        /// Gets the length in bytes of the stream.
        /// </summary>
        public long Length
        {
            get
            {
                return _length;
            }
        }

        /// <summary>
        /// Closes the current stream and releases any resources (such as sockets and
        //     file handles) associated with the current stream.
        /// </summary>
        public void Close()
        {
            Close(true);
        }

        /// <summary>
        /// Closes the current stream and releases any resources (such as sockets and
        //     file handles) associated with the current stream.
        /// </summary>
        /// <param name="commit">commit changes before closing the stream.</param>
        public void Close(bool commit)
        {
            if (commit)
            {
                Commit();
            }
            _fs.Close();
            _commitLog.Close();
        }

        /// <summary>
        /// Sets the current position of this stream to the given value.
        /// </summary>
        /// <param name="offset">The point relative to origin from which to begin seeking.</param>
        /// <param name="origin">Specifies the beginning, the end, or the current position as a reference point for origin, using a value of type SeekOrigin.</param>
        /// <returns>The new position in the stream.</returns>
        public long Seek(long offset, SeekOrigin origin)
        {
            if (origin == SeekOrigin.Current)
                offset = _position + offset;
            else if (origin == SeekOrigin.End)
                offset = _length + offset;

            _position = _fs.Seek(offset, SeekOrigin.Begin);
            return _position;
        }

        /// <summary>
        /// Reads a block of bytes from the stream and writes the data in a given buffer.
        /// </summary>
        /// <param name="array">When this method returns, contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The byte offset in array at which the read bytes will be placed.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <returns>The total number of bytes read into the buffer. This might be less than the number of bytes requested if that number of bytes are not currently available, or zero if the end of the stream is reached.</returns>
        public int Read(byte[] array, int offset, int count)
        {
            int readBytes = 0;

            while (readBytes < count)
            {
                long blockNo = _position / BLOCK_SIZE;
                int positionInBlock = (int)(_position % BLOCK_SIZE);

                byte[] block = new byte[BLOCK_SIZE];
                int blockSize = GetBlockCopy(blockNo, block);

                if (blockSize > 0)
                {
                    int n = Math.Min(count - readBytes, blockSize - positionInBlock);

                    Array.Copy(block, positionInBlock, array, offset, n);

                    positionInBlock += n;
                    readBytes += n;
                    _position += n;
                    offset += n;
                }
                else
                {
                    break;
                }
            }

            return readBytes;
        }

        /// <summary>
        /// Writes a block of bytes to this stream using data from a buffer.
        /// </summary>
        /// <param name="array">The buffer containing data to write to the stream.</param>
        /// <param name="offset"> The zero-based byte offset in array at which to begin copying bytes to the current stream.</param>
        /// <param name="count">The number of bytes to be written to the current stream.</param>
        public void Write(byte[] array, int offset, int count)
        {
            int copiedBytes = 0;

            while (copiedBytes < count)
            {
                long blockNo = _position / BLOCK_SIZE;
                int positionInBlock = (int)(_position % BLOCK_SIZE);

                byte[] BFIM = new byte[BLOCK_SIZE];
                GetBlockCopy(blockNo, BFIM);

                int n = Math.Min(count - copiedBytes, BLOCK_SIZE - positionInBlock);

                byte[] AFIM = new byte[BLOCK_SIZE];
                BFIM.CopyTo(AFIM, 0);

                Array.Copy(array, offset, AFIM, positionInBlock, n); // update block

                positionInBlock += n;

                _commitLog.LogWrite(_path, blockNo, positionInBlock, AFIM);

                AddBlockCopyToCache(blockNo, AFIM, positionInBlock);
               
                copiedBytes += n;
                offset += n;
                IncrementPosition(n);
            }
        }

        /// <summary>
        /// Commit all Write operations since last commit permanently to disk.
        /// </summary>
        public void Commit()
        {
            _commitLog.Commit();
        }

        /// <summary>
        /// Abort all Write operations since last commit.
        /// </summary>
        public void Abort()
        {
            _commitLog.Abort();
            _length = _fs.Length;
        }

        private void IncrementPosition(long incr)
        {
            _position += incr;
            if (_position > _length)
            {
                _length = _position;
            }
        }

        private int GetBlockCopy(long blockNo, byte[] block)
        {
            BlockCacheItem cacheItem = _cacheBlocksLRU.Get(blockNo);
            if (cacheItem != null)
            {
                cacheItem.block.CopyTo(block, 0);
                return cacheItem.count;
            }
            else
            {
                long diskAddress = blockNo * BLOCK_SIZE;
                if ((_fs.Position == diskAddress) || (_fs.Seek(diskAddress, SeekOrigin.Begin) == diskAddress))
                {
                    int n = _fs.Read(block, 0, BLOCK_SIZE);

                    // add block to cache
                    byte[] blockCopy = new byte[BLOCK_SIZE];
                    block.CopyTo(blockCopy, 0);
                    _cacheBlocksLRU.Add(blockNo, blockCopy, n, false);
                    return n;
                }
                else
                {
                    return -1; // unable to seek
                }
            }
        }

        private void AddBlockCopyToCache(long blockNo, byte[] block, int count)
        {
            byte[] blockCopy = new byte[BLOCK_SIZE];
            block.CopyTo(blockCopy, 0);

            BlockCacheItem cacheItem = _cacheBlocksLRU.Get(blockNo);
            if (cacheItem != null)
            {
                cacheItem.block = blockCopy;
                cacheItem.pinned = true;
                if (count > cacheItem.count)
                    cacheItem.count = count;
            }
            else
            {
                _cacheBlocksLRU.Add(blockNo, blockCopy, count, true);
            }
        }

        #region Failed Commit Simulation

        public enum FailedCommitCode { AFTER_LOG_BEGIN, AFTER_LOG_WRITE, AFTER_LOG_COMMIT,
        AFTER_DISK_WRITE, AFTER_LOG_END, CHECKPOINT_AFTER_LOG_BEGIN, CHECKPOINT_AFTER_LOG_WRITE,
        CHECKPOINT_AFTER_DISK_WRITE, CHECKPOINT_AFTER_LOG_END
        }

        public void __FailedCommitSimulation__(FailedCommitCode failedCommitCode)
        {
            _commitLog.__Test_Commit__(failedCommitCode);
        }

        #endregion
    }
}
