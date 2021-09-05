using System;
using System.Collections.Generic;
using Google.Protobuf;

namespace ConduitRawAPI.Database
{
    public interface ILmdbDatabase
    {
        public uint GetBlockNumber(byte[] blockHash);
        public ReadOnlySpan<byte> GetBlock(uint blockNum);
        public ReadOnlySpan<byte> GetMerkleTreeRow(byte[] blockHash, uint level);
        public uint GetLastBlockNumber();
        public void PutBlocks(List<Tuple<byte[], ulong, ulong>> batchedBlocks, ReadOnlySpan<byte> shmBuffer);
        public void PutMerkleTree(byte[] blockHash, Dictionary<uint, byte[]> merkleTree);
        public void PutTxOffsets(byte[] blockHash, ulong[] txOffsets);
        public ReadOnlySpan<ulong> GetTxOffsets(byte[] blockHash);
        public void PutBlockMetadata(List<Tuple<byte[], ulong>> batchedMetadata);
        public ulong GetBlockMetadata(byte[] blockHash);
    }
}