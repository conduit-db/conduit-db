using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Xml.Schema;
using Google.Protobuf;
using LightningDB;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ConduitRawAPI.Database
{
    public class LmdbDatabase: ILmdbDatabase, IDisposable
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
        private LightningDatabase _blocksDb;
        private LightningDatabase _blockNumsDb;
        private LightningDatabase _mtreeDb;
        private LightningDatabase _txOffsetsDb;
        private LightningDatabase _blockMetadataDb;

        public LmdbDatabase(ILogger<LmdbDatabase> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            var fileDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) ??
                          throw new ArgumentNullException(nameof(_lmdbPath));
            _lmdbPath = _configuration["LmdbDatabasePath"] ?? Path.Join(fileDir, "lmdb_data");
            _logger.LogInformation($"Got lmdbPath: {_lmdbPath}");
            long mapSize = 256_000_000_000;
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
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                var result = tx.Get(_blockNumsDb, blockHash);
                var theValue = result.value.CopyToNewArray();
                _logger.LogDebug($"theValue={BitConverter.ToString(theValue)}");
                
                return BitConverter.ToUInt32(theValue);
            }
        }

        public byte[] GetBlock(uint blockNum)
        {
            return new byte[1000];
        }

        public byte[] GetMerkleTreeRow(byte[] blockHash, uint level)
        {
            return new byte[256];
        }

        public uint GetLastBlockNumber()
        {
            return 999999999;
        }
        

        /// Puts to both _blockNumsDb and _blocksDB - At this stage the "putters" are only mocks to fill LMDB with some
        /// arbitrary data for testing the "getters". The gRPC API only needs to "get" from LMDB what the python
        /// ConduitRaw process "puts".
        public void PutBlocks(List<Tuple<byte[], ulong, ulong>> batchedBlocks)
        {
            using (var tx = _lmdbEnv.BeginTransaction())
            {
                byte[] blockNum = new byte[4];
                byte[] blockHash = new byte[32];
                byte[] rawBlock = new byte[10000];
                tx.Put(_blockNumsDb, blockHash, blockNum, PutOptions.NoOverwrite);
                tx.Put(_blocksDb, blockHash, rawBlock, PutOptions.AppendData | PutOptions.NoOverwrite);
                tx.Commit();
            }
        }

        public void PutMerkleTree(byte[] blockHash, Dictionary<uint, byte[]> merkleTree)
        {
            throw new System.NotImplementedException();
        }

        public void PutTxOffsets(byte[] blockHash, ulong[] txOffsets)
        {
            throw new System.NotImplementedException();
        }

        public ulong[] GetTxOffsets(byte[] blockHash)
        {
            return new ulong[100];
        }

        public void PutBlockMetadata(List<Tuple<byte[], ulong>> batchedMetadata)
        {
            throw new System.NotImplementedException();
        }

        public ulong GetBlockMetadata(byte[] blockHash)
        {
            ulong blockSizeBytes = 1000000000000;
            return blockSizeBytes;
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