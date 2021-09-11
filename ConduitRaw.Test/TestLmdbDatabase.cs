using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Moq;
using Xunit;
using ConduitRaw.Database;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace ConduitRaw.Test
{
    public class TestLmdbDatabase : IDisposable
    {
        private XunitLogger<LmdbDatabase> _logger;
        private readonly LmdbDatabase _lmdb;
        private readonly string _tempDir;

        public TestLmdbDatabase(ITestOutputHelper output)
        {
            _logger = new XunitLogger<LmdbDatabase>(output);
            _tempDir = GetRandomTempDir();

            var mockConfSection = new Mock<IConfigurationSection>();
            mockConfSection.SetupGet(m => m[It.Is<string>(s => s == "LmdbDatabasePath")]).Returns(_tempDir);
            var mockConfiguration = new Mock<IConfiguration>();
            mockConfiguration.Setup(a => a.GetSection(It.Is<string>(s => s == "AppSettings")))
                .Returns(mockConfSection.Object);

            _lmdb = new LmdbDatabase(_logger, mockConfiguration.Object);
        }

        private static string GetRandomTempDir()
        {
            string tempPath = Path.GetTempPath();
            var random = new Random();
            byte[] buff = new byte[16];
            random.NextBytes(buff);
            string tempDirPath = Path.Join(tempPath, BitConverter.ToString(buff));
            return tempDirPath;
        }

        [Fact]
        public void TestBlockStorage()
        {
            uint expectedBlockNum = 1; // because it is the first block entered into a fresh new database
            ulong expectedBlockSize = 64;
            string blockHashHex = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            byte[] blockHash = HexToByteArray(blockHashHex);
            byte[] expectedRawBlock =
                HexToByteArray(
                    "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
            // mock raw block is exactly 64 bytes long
            ulong startBlockOffset = 0;
            ulong endBlockOffset = 64;

            var batchedBlocks = new List<Tuple<byte[], ulong, ulong>>
            {
                new(blockHash, startBlockOffset, endBlockOffset)
            };
            // Test Puts to _blockNumsDb, _blockMetadataDb, _blocksDb
            _lmdb.PutBlocks(batchedBlocks, expectedRawBlock);

            // Test Getters
            uint blockNum = _lmdb.GetBlockNumber(blockHash);
            _logger.LogDebug($"_lmdb.GetBlockNumber result={blockNum}");
            Assert.Equal(expectedBlockNum, blockNum);

            ulong blockSize = _lmdb.GetBlockMetadata(blockHash);
            _logger.LogDebug($"_lmdb.GetBlockMetadata result={blockSize}");
            Assert.Equal(expectedBlockSize, blockSize);

            var rawBlock = _lmdb.GetBlock(blockNum).ToArray();
            _logger.LogDebug($"_lmdb.GetBlock result={BitConverter.ToString(rawBlock)}");
            Assert.Equal(expectedBlockNum, blockNum);
            Assert.Equal(expectedRawBlock, rawBlock);
        }

        [Fact]
        public void TestMerkleStorage()
        {
            byte[] blockHash = HexToByteArray("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
            byte[] txOne = HexToByteArray("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            byte[] txTwo = HexToByteArray("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
            byte[] baseLevel = txOne.Concat(txTwo).ToArray();
            byte[] merkleRoot = SHA256.HashData(baseLevel);
            var merkleTree = new Dictionary<uint, byte[]> {[1] = baseLevel, [0] = merkleRoot};

            // Test Puts to _mtreeDb
            _lmdb.PutMerkleTree(blockHash, merkleTree);

            // Test Gets from _mtreeDb
            var actualBaseLevel = _lmdb.GetMerkleTreeRow(blockHash, 1);
            var actualMerkleRoot = _lmdb.GetMerkleTreeRow(blockHash, 0);
            _logger.LogDebug($"_lmdb.GetMerkleTreeRow level 1 result={BitConverter.ToString(actualBaseLevel)}");
            _logger.LogDebug($"_lmdb.GetMerkleTreeRow merkleRoot result={BitConverter.ToString(actualMerkleRoot)}");
            Assert.Equal(baseLevel, actualBaseLevel);
            Assert.Equal(merkleRoot, actualMerkleRoot);
        }

        [Fact]
        public void TestTxOffsetStorage()
        {
            // Test Puts to _txOffsetsDb
            byte[] blockHash = HexToByteArray("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
            ulong[] txOffsets = {1, 2, 3};
            _lmdb.PutTxOffsets(blockHash, txOffsets);

            // Test Gets from _txOffsetsDb
            var actualTxOffsets = _lmdb.GetTxOffsets(blockHash);
            _logger.LogDebug($"_lmdb.GetTxOffsets result={string.Join(",", actualTxOffsets)}");
            Assert.Equal(txOffsets, actualTxOffsets);
        }

        public void Dispose()
        {
            _lmdb.Dispose();
            Directory.Delete(path: _tempDir, recursive: true);
        }

        private static byte[] HexToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                .Where(x => x % 2 == 0)
                .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                .ToArray();
        }
    }
}