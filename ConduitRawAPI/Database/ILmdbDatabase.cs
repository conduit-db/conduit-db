using System;
using System.Collections.Generic;
using Google.Protobuf;

namespace ConduitRawAPI.Database
{
    public interface ILmdbDatabase
    {
        public uint GetBlockNumber(byte[] blockHash);
        public byte[] GetBlock(uint blockNum);
        public byte[] GetMerkleTreeRow(byte[] blockHash, uint level);
        public uint GetLastBlockNumber();
        public void PutBlocks(List<Tuple<byte[], ulong, ulong>> batchedBlocks);
        public void PutMerkleTree(byte[] blockHash, Dictionary<uint, byte[]> merkleTree);
        public void PutTxOffsets(byte[] blockHash, ulong[] txOffsets);
        public ulong[] GetTxOffsets(byte[] blockHash);
        public void PutBlockMetadata(List<Tuple<byte[], ulong>> batchedMetadata);
        public ulong GetBlockMetadata(byte[] blockHash);
    }
}