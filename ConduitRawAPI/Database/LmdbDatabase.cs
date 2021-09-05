using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using LightningDB;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ConduitRawAPI.Database
{
    public class LmdbDatabase : ILmdbDatabase, IDisposable
    {
        private readonly ILogger<LmdbDatabase> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _lmdbPath;
        private LightningEnvironment _lmdbEnv;
        private string BLOCKS_DB = "blocks_db";
        private string BLOCK_NUMS_DB = "block_nums_db";
        private string MTREE_DB = "mtree_db";
        private string TX_OFFSETS_DB = "tx_offsets_db";
        private string BLOCK_METADATA_DB = "block_metadata_db";
        private readonly LightningDatabase _blocksDb;
        private readonly LightningDatabase _blockNumsDb;
        private readonly LightningDatabase _mtreeDb;
        private readonly LightningDatabase _txOffsetsDb;
        private readonly LightningDatabase _blockMetadataDb;

        public LmdbDatabase(ILogger<LmdbDatabase> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            var fileDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) ??
                          throw new ArgumentNullException(nameof(_lmdbPath));
            var appSettings = _configuration.GetSection("AppSettings");
            _lmdbPath = string.IsNullOrEmpty(appSettings["LmdbDatabasePath"])
                ? fileDir
                : appSettings["LmdbDatabasePath"];
            _logger.LogInformation($"Creating database at lmdbPath: {_lmdbPath}");
            long mapSize = 20_000_000_000;
            _lmdbEnv = new LightningEnvironment(_lmdbPath);
            _lmdbEnv.MaxDatabases = 5;
            _lmdbEnv.MapSize = mapSize;
            _lmdbEnv.MaxReaders = 10;
            _lmdbEnv.Open(EnvironmentOpenFlags.NoSync | EnvironmentOpenFlags.NoReadAhead);

            using (var tx = _lmdbEnv.BeginTransaction())
            {
                var dbConfig = new DatabaseConfiguration {Flags = DatabaseOpenFlags.Create};
                _blocksDb = tx.OpenDatabase(BLOCKS_DB, dbConfig);
                _blockNumsDb = tx.OpenDatabase(BLOCK_NUMS_DB, dbConfig);
                _mtreeDb = tx.OpenDatabase(MTREE_DB, dbConfig);
                _txOffsetsDb = tx.OpenDatabase(TX_OFFSETS_DB, dbConfig);
                _blockMetadataDb = tx.OpenDatabase(BLOCK_METADATA_DB, dbConfig);
                tx.Commit();
            }
        }

        public uint GetBlockNumber(byte[] blockHash)
        {
            using (var tx = _lmdbEnv.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                try
                {
                    var (resultCode, _, value) = tx.Get(_blockNumsDb, blockHash);
                    string humanReadable = BitConverter.ToString(value.CopyToNewArray());
                    if (resultCode == MDBResultCode.NotFound)
                    {
                        throw new KeyNotFoundException(
                            $"Block number for block hash {BitConverter.ToString(blockHash)} not found");
                    }

                    return BitConverter.ToUInt32(value.AsSpan());
                }
                finally
                {
                    tx.Commit(); // releases reader
                }
            }
        }

        public byte[] GetBlock(uint blockNum)
        {
            using (var tx = _lmdbEnv.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                try
                {
                    var (resultCode, _, value) = tx.Get(_blocksDb, BitConverter.GetBytes(blockNum));
                    if (resultCode == MDBResultCode.NotFound)
                    {
                        throw new KeyNotFoundException($"Block for block number {blockNum} not found");
                    }

                    return value.CopyToNewArray();
                }
                finally
                {
                    tx.Commit(); // releases reader
                }
            }
        }

        public byte[] GetMerkleTreeRow(byte[] blockHash, uint level)
        {
            using (var tx = _lmdbEnv.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                try
                {
                    var key = blockHash
                        .Concat(BitConverter.GetBytes(level))
                        .ToArray();
                    var (resultCode, _, value) = tx.Get(_mtreeDb, key);
                    if (resultCode == MDBResultCode.NotFound)
                    {
                        throw new KeyNotFoundException(
                            $"Merkle tree row for block hash {BitConverter.ToString(blockHash)} " +
                            $"and level {level} not found");
                    }

                    return value.CopyToNewArray();
                }
                finally
                {
                    tx.Commit(); // releases reader
                }
            }
        }

        public uint GetLastBlockNumber()
        {
            using (var tx = _lmdbEnv.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                try
                {
                    var cur = tx.CreateCursor(_blocksDb);
                    cur.Last();
                    var (resultCode, _, value) = cur.GetCurrent();
                    if (resultCode == MDBResultCode.Success)
                    {
                        return BitConverter.ToUInt32(value.AsSpan());
                    }

                    // Return zero because the intention is to determine which is the next
                    // index (key) to insert to the raw blocks table to remain sequential
                    return 0;
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        private uint GetUnixTimestamp()
        {
            return (uint) (DateTime.UtcNow.Subtract(DateTime.UnixEpoch)).TotalSeconds;
        }

        public void PutBlocks(List<Tuple<byte[], ulong, ulong>> batchedBlocks, byte[] buffer)
        {
            var t0 = GetUnixTimestamp();
            var lastBlockNum = GetLastBlockNumber();
            var nextBlockNum = lastBlockNum + 1;
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                try
                {
                    // Calculate the next blockNums in sequential order starting at the previous last blockNum
                    var blockNums = new List<uint>();
                    for (uint i = 0; i < batchedBlocks.Count; i++)
                    {
                        blockNums.Add(i + nextBlockNum);
                    }

                    // Write raw blocks in batch in append-only mode with sequential blockNum keys
                    // Additionally update the blockNumsDb with the mapping of blockHash -> blockNum 
                    // for later retrieval
                    var blockNumAndBlockRow =
                        blockNums.Zip(batchedBlocks, (blockNum, blockRow) => (blockNum, blockRow));

                    foreach (var (blockNum, blockRow) in blockNumAndBlockRow)
                    {
                        (var blockHash, var blockStartPos, var blockEndPos) = blockRow;
                        var key = BitConverter.GetBytes(blockNum);
                        var blockSize = blockEndPos - blockStartPos;
                        var value = buffer;

                        _logger.LogDebug(
                            $"Putting block entry blockNum={blockNum}, blockSize={value.ToArray().Length}");
                        tx.Put(_blocksDb, key, value.ToArray(), PutOptions.AppendData);
                        tx.Put(_blockNumsDb, blockHash, BitConverter.GetBytes(blockNum), PutOptions.NoOverwrite);
                        tx.Put(_blockMetadataDb, blockHash, BitConverter.GetBytes(blockSize));
                    }

                    var tDiff = GetUnixTimestamp() - t0;
                    if (batchedBlocks.Count > 0)
                    {
                        _logger.LogDebug($"Elapsed time for {batchedBlocks.Count} raw blocks took {tDiff} seconds");
                    }
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public static byte[] ConvertUInt64ToBytes(ulong value)
        {
            return BitConverter.GetBytes(value);
        }

        public void PutMerkleTree(byte[] blockHash, Dictionary<uint, byte[]> merkleTree)
        {
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                try
                {
                    foreach (var item in merkleTree.Reverse())
                    {
                        var level = item.Key;
                        var txHashes = item.Value;
                        var key = blockHash
                            .Concat(BitConverter.GetBytes(level))
                            .ToArray();
                        var value = txHashes;
                        tx.Put(_mtreeDb, key, value);
                    }
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public void PutTxOffsets(byte[] blockHash, ulong[] txOffsets)
        {
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                try
                {
                    var value = Array
                        .ConvertAll(txOffsets, ConvertUInt64ToBytes)
                        .SelectMany(x => x)
                        .ToArray();
                    tx.Put(_txOffsetsDb, blockHash, value);
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public ulong[] GetTxOffsets(byte[] blockHash)
        {
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                try
                {
                    var (resultCode, _, value) = tx.Get(_txOffsetsDb, blockHash);
                    if (resultCode == MDBResultCode.NotFound)
                    {
                        throw new KeyNotFoundException(
                            $"Tx offsets for block hash {BitConverter.ToString(blockHash)} " +
                            $"not found");
                    }

                    return MemoryMarshal.Cast<byte, ulong>(value.AsSpan()).ToArray();
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public void PutBlockMetadata(List<Tuple<byte[], ulong>> batchedMetadata)
        {
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                try
                {
                    foreach (var (blockHash, sizeBlock) in batchedMetadata)
                    {
                        tx.Put(_blockMetadataDb, blockHash, BitConverter.GetBytes(sizeBlock));
                    }
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public ulong GetBlockMetadata(byte[] blockHash)
        {
            using (var tx = _lmdbEnv.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                try
                {
                    var (resultCode, _, value) = tx.Get(_blockMetadataDb, blockHash);
                    if (resultCode == MDBResultCode.NotFound)
                    {
                        throw new KeyNotFoundException(
                            $"Block size for block hash {BitConverter.ToString(blockHash)} " +
                            $"not found");
                    }

                    return BitConverter.ToUInt64(value.AsSpan());
                }
                finally
                {
                    tx.Commit();
                }
            }
        }

        public void Dispose()
        {
            _logger.LogDebug("Disposing of Lmdb");
            _blocksDb.Dispose();
            _blockNumsDb.Dispose();
            _mtreeDb.Dispose();
            _txOffsetsDb.Dispose();
            _blockMetadataDb.Dispose();
            _lmdbEnv.Dispose();
            _lmdbEnv.Dispose();
            _lmdbEnv = null;
        }
    }
}