using Grpc.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConduitRawAPI.Database;
using Google.Protobuf;


namespace ConduitRawAPI
{
    public class ConduitRawService : ConduitRaw.ConduitRawBase
    {
        private readonly ILogger<ConduitRawService> _logger;
        private readonly ILmdbDatabase _lmdb;

        public ConduitRawService(
            ILogger<ConduitRawService> logger,
            ILmdbDatabase lmdbDatabase)
        {
            _logger = logger;
            _lmdb = lmdbDatabase;
            logger.LogInformation($"Got _lmdbEnv: {_lmdb}");
        }

        public override Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
        {
            _logger.LogDebug($"Ping got request with data: {request.Data}");
            return Task.FromResult(new PingResponse
            {
                Message = request.Data
            });
        }

        public override Task<BlockNumberResponse> GetBlockNumber(BlockNumberRequest request, ServerCallContext context)
        {
            _logger.LogDebug(
                $"GetBlockNumber got request with blockHash: " +
                $"{BitConverter.ToString(request.BlockHash.ToByteArray())}");
            try
            {
                return Task.FromResult(new BlockNumberResponse
                {
                    BlockNumber = _lmdb.GetBlockNumber(request.BlockHash.ToByteArray())
                });
            }
            catch (KeyNotFoundException ex)
            {
                throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
            }
        }

        public override Task<BlockResponse> GetBlock(BlockRequest request, ServerCallContext context)
        {
            _logger.LogDebug($"GetBlock got request with blockNumber: {request}");
            try
            {
                return Task.FromResult(new BlockResponse
                {
                    RawBlock = ByteString.CopyFrom(_lmdb.GetBlock(request.BlockNumber))
                });
            }
            catch (KeyNotFoundException ex)
            {
                throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
            }
        }

        public override Task<MerkleTreeRowResponse> GetMerkleTreeRow(MerkleTreeRowRequest request,
            ServerCallContext context)
        {
            _logger.LogDebug(
                $"GetMerkleTreeRow got request with blockHash: " +
                $"{BitConverter.ToString(request.BlockHash.ToByteArray())}");
            try
            {
                return Task.FromResult(new MerkleTreeRowResponse
                {
                    MtreeRow = ByteString.CopyFrom(_lmdb.GetMerkleTreeRow(request.ToByteArray(), request.Level))
                });
            }
            catch (KeyNotFoundException ex)
            {
                throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
            }
        }

        public override Task<TransactionOffsetsResponse> GetTransactionOffsets(TransactionOffsetsRequest request,
            ServerCallContext context)
        {
            _logger.LogDebug(
                $"GetTransactionOffsets got request with blockHash: " +
                $"{BitConverter.ToString(request.BlockHash.ToByteArray())}");
            try
            {
                return Task.FromResult(new TransactionOffsetsResponse
                {
                    TxOffsetsArray = {_lmdb.GetTxOffsets(request.ToByteArray())}
                });
            }
            catch (KeyNotFoundException ex)
            {
                throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
            }
        }

        public override Task<BlockMetadataResponse> GetBlockMetadata(BlockMetadataRequest request,
            ServerCallContext context)
        {
            _logger.LogDebug(
                $"GetBlockMetadata got request with blockHash: " +
                $"{BitConverter.ToString(request.BlockHash.ToByteArray())}");
            try
            {
                return Task.FromResult(new BlockMetadataResponse
                {
                    BlockSizeBytes = _lmdb.GetBlockMetadata(request.ToByteArray())
                });
            }
            catch (KeyNotFoundException ex)
            {
                throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
            }
        }
    }
}