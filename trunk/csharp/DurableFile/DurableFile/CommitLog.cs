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
    /// Commit Log class
    /// Write Ahead Logging (WAL)
    /// Recovery technique: Deferred Update
    /// 
    /// -----------------------------  0 byte
    /// | [first block - header]    |
    /// | checkpoint adr log_begin  |
    /// | checkpoint adr log_write  |
    /// | checkpoint adr log_end    |                       
    /// |                           |
    /// |                           |
    /// | checkpoint_adr            | 2048 bytes
    /// |                           |
    /// |                           |
    /// |                           |
    /// |                           |
    /// |                           |
    /// ----------------------------- 4096 bytes
    /// |   [second block]      
    /// |   START_LOG_POSITION      |
    /// |                           |
    /// |                           |
    /// |                           |
    /// </summary>
    class CommitLog
    {
        private const int BLOCK_SIZE = DurableFileStream.BLOCK_SIZE;

        private const int BEGIN_OPERATION = 1;
        private const int WRITE_OPERATION = 2;
        private const int COMMIT_OPERATION = 3;
        private const int END_OPERATION = 4;

        private const int START_LOG_POSITION = DurableFileStream.BLOCK_SIZE;

        private const long CHECKPOINT_POSITION = 2048;

        /// <summary>
        /// Write a new checkpoint every <code>RENEW_CHECKPOINT_RANGE</code> bytes.
        /// </summary>
        private const int RENEW_CHECKPOINT_THRESHOLD = 10 * 1024; // 10 KB

        /// <summary>
        /// Re-create a new log file every <code>RECREATE_LOG_RANGE</code> bytes.
        /// log file will not grow beyong <code>RECREATE_LOG_RANGE</code>
        /// </summary>
        private const int RECREATE_LOG_THRESHOLD = 50 * 1024 * 1024; // 50 MB

        private const int BUFFER_SIZE = 128 * 4096; // 512 kb
        private byte[] _buffer = new byte[BUFFER_SIZE];
        private int _bufferIdx = 0;

        byte[] _recoveryBuffer;

        private long _LSN = 0;

        private FileStream _fsLog;

        private Dictionary<long, WriteLogRecord> _writeLogRecordDict;

        private string _logPath;

        private DurableFileStream _dataFileStream;

        private int _transactionID = 0;

        private long _checkpointAddress = BLOCK_SIZE;

        public long CheckpointAddress
        {
            get
            {
                return _checkpointAddress;
            }
        }

        public CommitLog(DurableFileStream dataFileStream, bool create)
        {
            _dataFileStream = dataFileStream;
            _logPath = dataFileStream.Path + ".log";
            _writeLogRecordDict = new Dictionary<long, WriteLogRecord>();

            CreateOrOpenCommitFile(create);

            if (_fsLog.Length > 0)
            {
                // existing file
                _recoveryBuffer = new byte[8496];
                ReadHeaderBlock();
                ReadCheckpointAddress();
                Recover();
                _fsLog.Seek(Math.Max(START_LOG_POSITION, _fsLog.Length), SeekOrigin.Begin);
                _LSN = _fsLog.Position;
                _recoveryBuffer = null;
            }
        }

        public void Close()
        {
            _fsLog.Close();
        }

        public void LogWrite(string path, long blockNo, int count, byte[] AFIM)
        {
            WriteLogRecord writeLogRecord;
            if (_writeLogRecordDict.TryGetValue(blockNo, out writeLogRecord))
            {
                writeLogRecord.AFIM = AFIM;
                if (count > writeLogRecord.count)
                    writeLogRecord.count = count;
            }
            else
            {
                writeLogRecord = new WriteLogRecord();
                writeLogRecord.filename = path;
                writeLogRecord.blockNo = blockNo;
                writeLogRecord.count = count;
                writeLogRecord.AFIM = AFIM;
                _writeLogRecordDict.Add(blockNo, writeLogRecord);
            }
        }

        public void Commit()
        {
            if (_writeLogRecordDict.Count == 0)
                return;

            List<WriteLogRecord> writeLogRecordList = GetSortedWriteLogRecordList();

            NextTransactionID();

            long prevLSN = _LSN;

            prevLSN = LogBegin();

            foreach (WriteLogRecord record in writeLogRecordList)
            {
                prevLSN = LogWrite(prevLSN, record);
            }

            prevLSN = LogCommit(prevLSN);
            FlushBuffer();

            //------------------------------------------------------
            //   Finalize Transaction by writing the File
            //------------------------------------------------------
            WriteToDataFile(writeLogRecordList);
            _writeLogRecordDict.Clear();

            prevLSN = LogEnd(prevLSN);
            FlushBuffer();
            //------------------------------------------------------

            if (_fsLog.Length >= RECREATE_LOG_THRESHOLD)
            {
                CreateOrOpenCommitFile(true);  // delete log file then create a new one
            }

            if (_fsLog.Length - _checkpointAddress >= RENEW_CHECKPOINT_THRESHOLD)
            {
                WriteNewCheckpointAddress();  // change check point address
            }
        }

        public void __Test_Commit__(DurableFileStream.FailedCommitCode failedCommitCode)
        {
            if (_writeLogRecordDict.Count == 0)
                return;

            List<WriteLogRecord> writeLogRecordList = GetSortedWriteLogRecordList();

            NextTransactionID();

            long prevLSN = _LSN;

            prevLSN = LogBegin();
            FlushBuffer();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.AFTER_LOG_BEGIN)
                throw new Exception("Failed Commit: After Log Begin");

            foreach (WriteLogRecord record in writeLogRecordList)
            {
                prevLSN = LogWrite(prevLSN, record);
                FlushBuffer();
                if (failedCommitCode == DurableFileStream.FailedCommitCode.AFTER_LOG_WRITE)
                    throw new Exception("Failed Commit: After Log Write");
            }

            prevLSN = LogCommit(prevLSN);
            FlushBuffer();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.AFTER_LOG_COMMIT)
                throw new Exception("Failed Commit: After Log Commit");

            //------------------------------------------------------
            //   Finalize Transaction by writing the File
            //------------------------------------------------------
            WriteToDataFile(writeLogRecordList);
            _writeLogRecordDict.Clear();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.AFTER_DISK_WRITE)
                throw new Exception("Failed Commit: After Disk Write");

            prevLSN = LogEnd(prevLSN);
            FlushBuffer();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.AFTER_LOG_END)
                throw new Exception("Failed Commit: After Log End");

            //------------------------------------------------------

            if (_fsLog.Length >= RECREATE_LOG_THRESHOLD)
            {
                CreateOrOpenCommitFile(true);  // delete log file then create a new one
            }

            if (_fsLog.Length - _checkpointAddress >= RENEW_CHECKPOINT_THRESHOLD)
            {
                __Test_WriteNewCheckpointAddress__(failedCommitCode);   // change check point address
            }
        }

        private void __Test_WriteNewCheckpointAddress__(DurableFileStream.FailedCommitCode failedCommitCode)
        {
            NextTransactionID();

            // clear first block on disk before writing log
            _fsLog.Seek(0, SeekOrigin.Begin); // go to first block
            ClearBuffer();
            _bufferIdx = BLOCK_SIZE;
            FlushBuffer();

            // write log to the first block after clearing
            _fsLog.Seek(0, SeekOrigin.Begin); // go to first block

            WriteLogRecord ckpWriteRecord = new WriteLogRecord();
            ckpWriteRecord.filename = _logPath;
            ckpWriteRecord.blockNo = 1;
            ckpWriteRecord.count = 8;
            ckpWriteRecord.AFIM = BitConverter.GetBytes(_fsLog.Length);

            _LSN = 0;
            long prev_lsn = LogBegin();
            FlushBuffer();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_BEGIN)
                throw new Exception("Failed Commit: Checkpoint After Log Begin");

            prev_lsn = LogWrite(prev_lsn, ckpWriteRecord);
            FlushBuffer();
            long lastLogPos = _fsLog.Position;

            if (failedCommitCode == DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_WRITE)
                throw new Exception("Failed Commit: Checkpoint After Log Write");

            _fsLog.Seek(BLOCK_SIZE, SeekOrigin.Begin);
            _fsLog.Write(ckpWriteRecord.AFIM, 0, 8);
            _fsLog.Flush(true);

            if (failedCommitCode == DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_DISK_WRITE)
                throw new Exception("Failed Commit: Checkpoint After Disk Write");

            _fsLog.Seek(lastLogPos, SeekOrigin.Begin); // go to first block
            prev_lsn = LogEnd(prev_lsn);
            FlushBuffer();

            if (failedCommitCode == DurableFileStream.FailedCommitCode.CHECKPOINT_AFTER_LOG_END)
                throw new Exception("Failed Commit: Checkpoint After Log End");

            _checkpointAddress = _fsLog.Length;

            // reset LSN
            _fsLog.Seek(0, SeekOrigin.End);
            _LSN = _fsLog.Position;
        }

        public void Abort()
        {
            if (_writeLogRecordDict.Count == 0)
                return;

            foreach (KeyValuePair<long, WriteLogRecord> entry in _writeLogRecordDict)
            {
                _dataFileStream._cacheBlocksLRU.Remove(entry.Value.blockNo);
            }

            _writeLogRecordDict.Clear();
        }

        private void CreateOrOpenCommitFile(bool create)
        {
            FileMode fileMode = FileMode.OpenOrCreate;
            if (create)
                fileMode = FileMode.Create;

            if (_fsLog != null)
                _fsLog.Close();

            _fsLog = new FileStream(_logPath, fileMode, FileAccess.ReadWrite, FileShare.ReadWrite,
                16 * BLOCK_SIZE, FileOptions.WriteThrough);

            if (_fsLog.Length == 0)
            {
                _fsLog.Seek(START_LOG_POSITION, SeekOrigin.Begin);
                _checkpointAddress = START_LOG_POSITION;
                _LSN = _fsLog.Position;
            }

            _bufferIdx = 0;
            ClearBuffer();
        }

        private void Recover()
        {
            if (_checkpointAddress == _fsLog.Length)
            {
                return; // no changes
            }

            _fsLog.Seek(_checkpointAddress, SeekOrigin.Begin);

            List<WriteLogRecord> transWriteRecordList = new List<WriteLogRecord>(100);
            long prevLSN;
            long currTransactionID;

            while (_fsLog.Position < _fsLog.Length)
            {
                transWriteRecordList.Clear();

                //
                // expecting log begin
                //
                LogRecord logRecord = ParseLogRecord(_dataFileStream.Path);
                if (logRecord == null || logRecord.operation != BEGIN_OPERATION)
                {
                    break; // failure: NO-UNDO : ignore
                }
                prevLSN = logRecord.lsn;
                currTransactionID = logRecord.transactionID;

                //
                // expecting write log
                //
                logRecord = ParseLogRecord(_dataFileStream.Path);
                if (logRecord == null || logRecord.transactionID != currTransactionID
                    || logRecord.prev_lsn != prevLSN || logRecord.lsn < logRecord.prev_lsn)
                {
                    break; // failure
                }
                prevLSN = logRecord.lsn;

                bool failureInWriteOp = false;
                while (logRecord.operation == WRITE_OPERATION)
                {
                    WriteLogRecord writeLogRecord = (WriteLogRecord)logRecord;

                    transWriteRecordList.Add(writeLogRecord);

                    logRecord = ParseLogRecord(_dataFileStream.Path);
                    if (logRecord == null || logRecord.transactionID != currTransactionID
                        || logRecord.prev_lsn != prevLSN || logRecord.lsn < logRecord.prev_lsn)
                    {
                        failureInWriteOp = true;
                        break; // failure: NO-UNDO : ignore
                    }
                    prevLSN = logRecord.lsn;
                }
                if (failureInWriteOp)
                {
                    break; // failure: NO-UNDO : ignore
                }

                //
                // expecting log commit
                //
                if (logRecord.operation != COMMIT_OPERATION)
                {
                    break; // failure: NO-UNDO : ignore
                }
                prevLSN = logRecord.lsn;

                // between log_commit and log_end, the system write to data file

                //
                // expecting log end
                //
                logRecord = ParseLogRecord(_dataFileStream.Path);
                if (logRecord == null || logRecord.transactionID != currTransactionID
                    || logRecord.prev_lsn != prevLSN || logRecord.lsn < logRecord.prev_lsn
                    || logRecord.operation != END_OPERATION)
                {
                    //
                    // committed transaction but not ended while writing the data file --> REDO
                    //
                    WriteToDataFile(transWriteRecordList); // failure: REDO
                    break;
                }
            }

            //
            //  Write new checkpoint == end of file
            //
            WriteNewCheckpointAddress();
        }

        private LogRecord ParseLogRecord(string expectedFilename)
        {
            long rowPosition = _fsLog.Position;

            if (_fsLog.Read(_recoveryBuffer, 0, 32) != 32) // read first 32 bytes
                return null;

            long lsn = BitConverter.ToInt64(_recoveryBuffer, 0);
            if (lsn != rowPosition)
                return null;

            int recordLength = BitConverter.ToInt32(_recoveryBuffer, 8);
            if (recordLength < 32 || recordLength > 8496)
                return null;

            long prev_lsn = BitConverter.ToInt64(_recoveryBuffer, 12);
            if (prev_lsn > lsn)
                return null;

            int transactionID = BitConverter.ToInt32(_recoveryBuffer, 20);

            int operation = BitConverter.ToInt32(_recoveryBuffer, 24);
            if (operation < 1 || operation > 4)
                return null;

            if (operation == WRITE_OPERATION)
            {
                int filenameLength = BitConverter.ToInt32(_recoveryBuffer, 28);

                if ((filenameLength > 255) || (32 + filenameLength > recordLength))
                    return null;

                if (_fsLog.Read(_recoveryBuffer, 32, recordLength - 32) != recordLength - 32)
                    return null;

                string filename = System.Text.Encoding.UTF8.GetString(_recoveryBuffer, 32, filenameLength);
                int idx = 32 + filenameLength;
                if (filename != expectedFilename)
                    return null;

                if (idx + 12 > recordLength)
                    return null;

                long blockNo = BitConverter.ToInt64(_recoveryBuffer, idx);
                int count = BitConverter.ToInt32(_recoveryBuffer, idx + 8);
                idx += 12;

                if (idx + count + 4 > recordLength)
                    return null;

                byte[] AFIM = new byte[count];
                Array.Copy(_recoveryBuffer, idx, AFIM, 0, count);
                idx += count;

                UInt32 read_crc32 = BitConverter.ToUInt32(_recoveryBuffer, idx);
                UInt32 computed_crc32 = CRC32.Compute(_recoveryBuffer, 0, idx);
                if (read_crc32 != computed_crc32)
                {
                    return null;
                }

                WriteLogRecord writeLogRecord = new WriteLogRecord();
                writeLogRecord.lsn = lsn;
                writeLogRecord.prev_lsn = prev_lsn;
                writeLogRecord.transactionID = transactionID;
                writeLogRecord.operation = operation;
                writeLogRecord.filename = filename;
                writeLogRecord.blockNo = blockNo;
                writeLogRecord.count = count;
                writeLogRecord.AFIM = AFIM;
                return writeLogRecord;
            }
            else
            {
                UInt32 read_crc32 = BitConverter.ToUInt32(_recoveryBuffer, 28);
                UInt32 computed_crc32 = CRC32.Compute(_recoveryBuffer, 0, 28);
                if (read_crc32 != computed_crc32)
                {
                    return null;
                }

                LogRecord logRecord = new LogRecord();
                logRecord.lsn = lsn;
                logRecord.prev_lsn = prev_lsn;
                logRecord.transactionID = transactionID;
                logRecord.operation = operation;
                return logRecord;
            }
        }

        private bool ReadHeaderBlock()
        {
            byte[] buffer = new byte[BUFFER_SIZE];

            //
            // Read First Block
            //
            _fsLog.Seek(0, SeekOrigin.Begin);

            //**************************************************
            //
            // expecting log begin
            //
            LogRecord logRecord = ParseLogRecord(_logPath);
            if (logRecord == null || logRecord.operation != BEGIN_OPERATION ||
                logRecord.lsn != 0 || logRecord.prev_lsn != 0)
            {
                return false; // failure
            }

            long prevLSN = logRecord.lsn;
            int currTransactionID = logRecord.transactionID;

            //
            // expecting write log
            //
            logRecord = ParseLogRecord(_logPath);
            if (logRecord == null || logRecord.transactionID != currTransactionID
                || logRecord.prev_lsn != prevLSN || logRecord.lsn < logRecord.prev_lsn ||
                logRecord.operation != WRITE_OPERATION)
            {
                return false; // failure
            }
            WriteLogRecord writeLogRecord = (WriteLogRecord)logRecord;
            prevLSN = logRecord.lsn;

            //
            // expecting log end
            //
            logRecord = ParseLogRecord(_logPath);
            if (logRecord == null || logRecord.transactionID != currTransactionID
                || logRecord.prev_lsn != prevLSN || logRecord.lsn < logRecord.prev_lsn
                || logRecord.operation != END_OPERATION)
            {
                //
                // committed transaction but not ended while writing the data file --> REDO
                //
                return RedoCheckpointAddress(writeLogRecord.blockNo, writeLogRecord.count, writeLogRecord.AFIM);
            }

            return true;
        }

        private bool RedoCheckpointAddress(long blockNo, int count, byte[] AFIM)
        {
            _fsLog.Seek(blockNo * BLOCK_SIZE, SeekOrigin.Begin);
            _fsLog.Write(AFIM, 0, count);
            _fsLog.Flush(true);
            return true;
        }

        private void ReadCheckpointAddress()
        {
            _fsLog.Seek(CHECKPOINT_POSITION, SeekOrigin.Begin);

            byte[] buffer = new byte[8];
            _fsLog.Read(buffer, 0, 8);

            _checkpointAddress = BitConverter.ToInt64(buffer, 0);

            if (_checkpointAddress < START_LOG_POSITION)
                _checkpointAddress = START_LOG_POSITION;
        }

        private void WriteNewCheckpointAddress()
        {
            NextTransactionID();

            // clear first block on disk before writing log
            _fsLog.Seek(0, SeekOrigin.Begin); // go to first block
            ClearBuffer();
            _bufferIdx = 0;
            //_bufferIdx = BLOCK_SIZE;
            //FlushBuffer();

            // write log to the first block after clearing
            _fsLog.Seek(0, SeekOrigin.Begin); // go to first block

            WriteLogRecord ckpWriteRecord = new WriteLogRecord();
            ckpWriteRecord.filename = _logPath;
            ckpWriteRecord.blockNo = 1;
            ckpWriteRecord.count = 8;
            ckpWriteRecord.AFIM = BitConverter.GetBytes(_fsLog.Length);

            _LSN = 0;
            long prev_lsn = LogBegin();
            prev_lsn = LogWrite(prev_lsn, ckpWriteRecord);
            FlushBuffer();
            long lastLogPos = _fsLog.Position;

            // write checkpoint address to log disk
            _fsLog.Seek(CHECKPOINT_POSITION, SeekOrigin.Begin);
            _fsLog.Write(ckpWriteRecord.AFIM, 0, 8);
            _fsLog.Flush(true);

            _fsLog.Seek(lastLogPos, SeekOrigin.Begin); // return to last position
            prev_lsn = LogEnd(prev_lsn);
            FlushBuffer();

            _checkpointAddress = _fsLog.Length;

            // reset LSN
            _fsLog.Seek(0, SeekOrigin.End);
            _LSN = _fsLog.Position;
        }

        private long LogBegin()
        {
            long lsn = _LSN;

            byte[] recordBytes = new byte[32];

            byte[] inBytes = BitConverter.GetBytes(lsn); // LSN: 8 bytes
            Array.Copy(inBytes, 0, recordBytes, 0, 8);

            inBytes = BitConverter.GetBytes(recordBytes.Length); // record Length: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 8, 4);

            inBytes = BitConverter.GetBytes(lsn);  // prevLSN: 8 bytes (equal to lsn)
            Array.Copy(inBytes, 0, recordBytes, 12, 8);

            inBytes = BitConverter.GetBytes(_transactionID); // TransactionID: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 20, 4);

            inBytes = BitConverter.GetBytes(BEGIN_OPERATION); // Operation: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 24, 4);

            inBytes = BitConverter.GetBytes(CRC32.Compute(recordBytes, 0, 28)); // CRC32: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 28, 4);

            WriteToBuffer(recordBytes, 32);

            return lsn;
        }

        private long LogCommit(long prevLSN)
        {
            long lsn = _LSN;

            byte[] recordBytes = new byte[32];

            byte[] inBytes = BitConverter.GetBytes(lsn); // LSN: 8 bytes
            Array.Copy(inBytes, 0, recordBytes, 0, 8);

            inBytes = BitConverter.GetBytes(recordBytes.Length); // record Length: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 8, 4);

            inBytes = BitConverter.GetBytes(prevLSN);  // prevLSN: 8 bytes (equal to lsn)
            Array.Copy(inBytes, 0, recordBytes, 12, 8);

            inBytes = BitConverter.GetBytes(_transactionID); // TransactionID: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 20, 4);

            inBytes = BitConverter.GetBytes(COMMIT_OPERATION); // Operation: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 24, 4);

            inBytes = BitConverter.GetBytes(CRC32.Compute(recordBytes, 0, 28)); // CRC32: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 28, 4);

            WriteToBuffer(recordBytes, 32);

            return lsn;
        }

        private long LogEnd(long prevLSN)
        {
            long lsn = _LSN;

            byte[] recordBytes = new byte[32];

            byte[] inBytes = BitConverter.GetBytes(lsn); // LSN: 8 bytes
            Array.Copy(inBytes, 0, recordBytes, 0, 8);

            inBytes = BitConverter.GetBytes(recordBytes.Length); // record Length: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 8, 4);

            inBytes = BitConverter.GetBytes(prevLSN);  // prevLSN: 8 bytes (equal to lsn)
            Array.Copy(inBytes, 0, recordBytes, 12, 8);

            inBytes = BitConverter.GetBytes(_transactionID); // TransactionID: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 20, 4);

            inBytes = BitConverter.GetBytes(END_OPERATION); // Operation: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 24, 4);

            inBytes = BitConverter.GetBytes(CRC32.Compute(recordBytes, 0, 28)); // CRC32: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 28, 4);

            WriteToBuffer(recordBytes, 32);

            return lsn;
        }

        private long LogWrite(long prevLSN, WriteLogRecord record)
        {
            long lsn = _LSN;

            byte[] fileNameInBytes = System.Text.Encoding.UTF8.GetBytes(record.filename);
            int filenameLen = fileNameInBytes.Length;

            byte[] recordBytes = new byte[48 + filenameLen + record.count];

            byte[] inBytes = BitConverter.GetBytes(lsn); // LSN: 8 bytes
            Array.Copy(inBytes, 0, recordBytes, 0, 8);

            inBytes = BitConverter.GetBytes(recordBytes.Length); // record Length: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 8, 4);

            inBytes = BitConverter.GetBytes(prevLSN);  // prevLSN: 8 bytes (equal to lsn)
            Array.Copy(inBytes, 0, recordBytes, 12, 8);

            inBytes = BitConverter.GetBytes(_transactionID); // TransactionID: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 20, 4);

            inBytes = BitConverter.GetBytes(WRITE_OPERATION); // Operation: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 24, 4);

            inBytes = BitConverter.GetBytes(filenameLen); // filename length in bytes: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, 28, 4);

            Array.Copy(fileNameInBytes, 0, recordBytes, 32, filenameLen); // filename: inBytes.Length bytes

            int idx = 32 + filenameLen;

            inBytes = BitConverter.GetBytes(record.blockNo); // blockNo: 8 bytes
            Array.Copy(inBytes, 0, recordBytes, idx, 8);
            idx += 8;

            inBytes = BitConverter.GetBytes(record.count); // count: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, idx, 4);
            idx += 4;

            Array.Copy(record.AFIM, 0, recordBytes, idx, record.count); // AFIM: <record.count> bytes
            idx += record.count;

            inBytes = BitConverter.GetBytes(CRC32.Compute(recordBytes, 0, idx)); // CRC32: 4 bytes
            Array.Copy(inBytes, 0, recordBytes, idx, 4);
            idx += 4;

            WriteToBuffer(recordBytes, idx);

            return lsn;
        }

        private void WriteToBuffer(byte[] array, int count)
        {
            int writtenBytes = 0;
            int offset = 0;

            while (writtenBytes < count)
            {
                int n = Math.Min(BUFFER_SIZE - _bufferIdx, count - writtenBytes);
                Array.Copy(array, offset, _buffer, _bufferIdx, n);
                _bufferIdx += n;
                _LSN += n;
                offset += n;

                if (_bufferIdx == BUFFER_SIZE)
                {
                    FlushBuffer();
                }

                writtenBytes += n;
            }

            if (_bufferIdx == BUFFER_SIZE)
            {
                FlushBuffer();
            }
        }

        private void FlushBuffer()
        {
            if (_bufferIdx > 0)
            {
                _fsLog.Write(_buffer, 0, _bufferIdx);
                _fsLog.Flush(true);

                ClearBuffer();
                _bufferIdx = 0;
            }
        }

        private void ClearBuffer()
        {
            Array.Clear(_buffer, 0, BUFFER_SIZE);
        }

        private void WriteToDataFile(List<WriteLogRecord> writeRecordList)
        {
            foreach (WriteLogRecord record in writeRecordList)
            {
                long position = record.blockNo * BLOCK_SIZE;
                if (_dataFileStream.FileStream.Position != position)
                {
                    _dataFileStream.FileStream.Seek(position, SeekOrigin.Begin);
                }
                _dataFileStream.FileStream.Write(record.AFIM, 0, record.count);
                _dataFileStream._cacheBlocksLRU.UnpinBlock(record.blockNo);
            }
            _dataFileStream.FileStream.Flush(true);
        }

        private void NextTransactionID()
        {
            if (_transactionID == Int32.MaxValue) { _transactionID = 0; }
            _transactionID++;
        }

        private List<WriteLogRecord> GetSortedWriteLogRecordList()
        {
            List<WriteLogRecord> writeLogRecordList = new List<WriteLogRecord>(_writeLogRecordDict.Count);

            foreach (KeyValuePair<long, WriteLogRecord> entry in _writeLogRecordDict)
            {
                writeLogRecordList.Add(entry.Value);
            }

            writeLogRecordList.Sort(); // sort by block no

            return writeLogRecordList;
        }

        class LogRecord
        {
            public long lsn;
            public long prev_lsn;
            public int transactionID;
            public int operation;
        }

        class WriteLogRecord : LogRecord, IComparable
        {
            public string filename;
            public long blockNo;

            /// <summary>
            /// number of used bytes in the block
            /// </summary>
            public int count;

            /// <summary>
            /// Block After Image
            /// </summary>
            public byte[] AFIM;

            int System.IComparable.CompareTo(object o)
            {
                WriteLogRecord wlr = (WriteLogRecord)o;
                if (this.blockNo > wlr.blockNo) return 1;
                if (this.blockNo < wlr.blockNo) return -1;
                return 0;
            }
        }
    }
}
