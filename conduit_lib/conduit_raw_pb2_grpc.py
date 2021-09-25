# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import conduit_raw_pb2 as conduit__raw__pb2


class ConduitRawStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Ping = channel.unary_unary(
                '/conduit_raw.ConduitRaw/Ping',
                request_serializer=conduit__raw__pb2.PingRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.PingResponse.FromString,
                )
        self.Stop = channel.unary_unary(
                '/conduit_raw.ConduitRaw/Stop',
                request_serializer=conduit__raw__pb2.StopRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.StopResponse.FromString,
                )
        self.GetBlockNumber = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlockNumber',
                request_serializer=conduit__raw__pb2.BlockNumberRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockNumberResponse.FromString,
                )
        self.GetBlockNumberBatched = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlockNumberBatched',
                request_serializer=conduit__raw__pb2.BlockNumberBatchedRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockNumberBatchedResponse.FromString,
                )
        self.GetBlock = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlock',
                request_serializer=conduit__raw__pb2.BlockRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockResponse.FromString,
                )
        self.GetBlockBatched = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlockBatched',
                request_serializer=conduit__raw__pb2.BlockBatchedRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockBatchedResponse.FromString,
                )
        self.GetMerkleTreeRow = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetMerkleTreeRow',
                request_serializer=conduit__raw__pb2.MerkleTreeRowRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.MerkleTreeRowResponse.FromString,
                )
        self.GetTransactionOffsets = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetTransactionOffsets',
                request_serializer=conduit__raw__pb2.TransactionOffsetsRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.TransactionOffsetsResponse.FromString,
                )
        self.GetTransactionOffsetsBatched = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetTransactionOffsetsBatched',
                request_serializer=conduit__raw__pb2.TransactionOffsetsBatchedRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.TransactionOffsetsBatchedResponse.FromString,
                )
        self.GetBlockMetadata = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlockMetadata',
                request_serializer=conduit__raw__pb2.BlockMetadataRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockMetadataResponse.FromString,
                )
        self.GetBlockMetadataBatched = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetBlockMetadataBatched',
                request_serializer=conduit__raw__pb2.BlockMetadataBatchedRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockMetadataBatchedResponse.FromString,
                )
        self.GetHeadersBatched = channel.unary_unary(
                '/conduit_raw.ConduitRaw/GetHeadersBatched',
                request_serializer=conduit__raw__pb2.BlockHeadersBatchedRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockHeadersBatchedResponse.FromString,
                )


class ConduitRawServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Ping(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Stop(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockNumber(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockNumberBatched(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlock(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockBatched(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetMerkleTreeRow(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTransactionOffsets(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTransactionOffsetsBatched(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockMetadata(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockMetadataBatched(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetHeadersBatched(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ConduitRawServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Ping': grpc.unary_unary_rpc_method_handler(
                    servicer.Ping,
                    request_deserializer=conduit__raw__pb2.PingRequest.FromString,
                    response_serializer=conduit__raw__pb2.PingResponse.SerializeToString,
            ),
            'Stop': grpc.unary_unary_rpc_method_handler(
                    servicer.Stop,
                    request_deserializer=conduit__raw__pb2.StopRequest.FromString,
                    response_serializer=conduit__raw__pb2.StopResponse.SerializeToString,
            ),
            'GetBlockNumber': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockNumber,
                    request_deserializer=conduit__raw__pb2.BlockNumberRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockNumberResponse.SerializeToString,
            ),
            'GetBlockNumberBatched': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockNumberBatched,
                    request_deserializer=conduit__raw__pb2.BlockNumberBatchedRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockNumberBatchedResponse.SerializeToString,
            ),
            'GetBlock': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlock,
                    request_deserializer=conduit__raw__pb2.BlockRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockResponse.SerializeToString,
            ),
            'GetBlockBatched': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockBatched,
                    request_deserializer=conduit__raw__pb2.BlockBatchedRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockBatchedResponse.SerializeToString,
            ),
            'GetMerkleTreeRow': grpc.unary_unary_rpc_method_handler(
                    servicer.GetMerkleTreeRow,
                    request_deserializer=conduit__raw__pb2.MerkleTreeRowRequest.FromString,
                    response_serializer=conduit__raw__pb2.MerkleTreeRowResponse.SerializeToString,
            ),
            'GetTransactionOffsets': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTransactionOffsets,
                    request_deserializer=conduit__raw__pb2.TransactionOffsetsRequest.FromString,
                    response_serializer=conduit__raw__pb2.TransactionOffsetsResponse.SerializeToString,
            ),
            'GetTransactionOffsetsBatched': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTransactionOffsetsBatched,
                    request_deserializer=conduit__raw__pb2.TransactionOffsetsBatchedRequest.FromString,
                    response_serializer=conduit__raw__pb2.TransactionOffsetsBatchedResponse.SerializeToString,
            ),
            'GetBlockMetadata': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockMetadata,
                    request_deserializer=conduit__raw__pb2.BlockMetadataRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockMetadataResponse.SerializeToString,
            ),
            'GetBlockMetadataBatched': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockMetadataBatched,
                    request_deserializer=conduit__raw__pb2.BlockMetadataBatchedRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockMetadataBatchedResponse.SerializeToString,
            ),
            'GetHeadersBatched': grpc.unary_unary_rpc_method_handler(
                    servicer.GetHeadersBatched,
                    request_deserializer=conduit__raw__pb2.BlockHeadersBatchedRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockHeadersBatchedResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'conduit_raw.ConduitRaw', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ConduitRaw(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Ping(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/Ping',
            conduit__raw__pb2.PingRequest.SerializeToString,
            conduit__raw__pb2.PingResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Stop(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/Stop',
            conduit__raw__pb2.StopRequest.SerializeToString,
            conduit__raw__pb2.StopResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlockNumber(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlockNumber',
            conduit__raw__pb2.BlockNumberRequest.SerializeToString,
            conduit__raw__pb2.BlockNumberResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlockNumberBatched(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlockNumberBatched',
            conduit__raw__pb2.BlockNumberBatchedRequest.SerializeToString,
            conduit__raw__pb2.BlockNumberBatchedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlock(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlock',
            conduit__raw__pb2.BlockRequest.SerializeToString,
            conduit__raw__pb2.BlockResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlockBatched(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlockBatched',
            conduit__raw__pb2.BlockBatchedRequest.SerializeToString,
            conduit__raw__pb2.BlockBatchedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetMerkleTreeRow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetMerkleTreeRow',
            conduit__raw__pb2.MerkleTreeRowRequest.SerializeToString,
            conduit__raw__pb2.MerkleTreeRowResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetTransactionOffsets(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetTransactionOffsets',
            conduit__raw__pb2.TransactionOffsetsRequest.SerializeToString,
            conduit__raw__pb2.TransactionOffsetsResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetTransactionOffsetsBatched(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetTransactionOffsetsBatched',
            conduit__raw__pb2.TransactionOffsetsBatchedRequest.SerializeToString,
            conduit__raw__pb2.TransactionOffsetsBatchedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlockMetadata(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlockMetadata',
            conduit__raw__pb2.BlockMetadataRequest.SerializeToString,
            conduit__raw__pb2.BlockMetadataResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetBlockMetadataBatched(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetBlockMetadataBatched',
            conduit__raw__pb2.BlockMetadataBatchedRequest.SerializeToString,
            conduit__raw__pb2.BlockMetadataBatchedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetHeadersBatched(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conduit_raw.ConduitRaw/GetHeadersBatched',
            conduit__raw__pb2.BlockHeadersBatchedRequest.SerializeToString,
            conduit__raw__pb2.BlockHeadersBatchedResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
