# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import conduit_raw_pb2 as conduit__raw__pb2


class ConduitRawStub(object):
    """The greeting service definition.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Ping = channel.unary_unary(
                '/greet.ConduitRaw/Ping',
                request_serializer=conduit__raw__pb2.PingRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.PingResponse.FromString,
                )
        self.GetBlockNumber = channel.unary_unary(
                '/greet.ConduitRaw/GetBlockNumber',
                request_serializer=conduit__raw__pb2.BlockNumberRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockNumberResponse.FromString,
                )
        self.GetBlock = channel.unary_unary(
                '/greet.ConduitRaw/GetBlock',
                request_serializer=conduit__raw__pb2.BlockRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockResponse.FromString,
                )
        self.GetMerkleTreeRow = channel.unary_unary(
                '/greet.ConduitRaw/GetMerkleTreeRow',
                request_serializer=conduit__raw__pb2.MerkleTreeRowRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.MerkleTreeRowResponse.FromString,
                )
        self.GetTransactionOffsets = channel.unary_unary(
                '/greet.ConduitRaw/GetTransactionOffsets',
                request_serializer=conduit__raw__pb2.TransactionOffsetsRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.TransactionOffsetsResponse.FromString,
                )
        self.GetBlockMetadata = channel.unary_unary(
                '/greet.ConduitRaw/GetBlockMetadata',
                request_serializer=conduit__raw__pb2.BlockMetadataRequest.SerializeToString,
                response_deserializer=conduit__raw__pb2.BlockMetadataResponse.FromString,
                )


class ConduitRawServicer(object):
    """The greeting service definition.
    """

    def Ping(self, request, context):
        """Sends a greeting
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlockNumber(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetBlock(self, request, context):
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

    def GetBlockMetadata(self, request, context):
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
            'GetBlockNumber': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockNumber,
                    request_deserializer=conduit__raw__pb2.BlockNumberRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockNumberResponse.SerializeToString,
            ),
            'GetBlock': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlock,
                    request_deserializer=conduit__raw__pb2.BlockRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockResponse.SerializeToString,
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
            'GetBlockMetadata': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBlockMetadata,
                    request_deserializer=conduit__raw__pb2.BlockMetadataRequest.FromString,
                    response_serializer=conduit__raw__pb2.BlockMetadataResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'greet.ConduitRaw', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ConduitRaw(object):
    """The greeting service definition.
    """

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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/Ping',
            conduit__raw__pb2.PingRequest.SerializeToString,
            conduit__raw__pb2.PingResponse.FromString,
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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/GetBlockNumber',
            conduit__raw__pb2.BlockNumberRequest.SerializeToString,
            conduit__raw__pb2.BlockNumberResponse.FromString,
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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/GetBlock',
            conduit__raw__pb2.BlockRequest.SerializeToString,
            conduit__raw__pb2.BlockResponse.FromString,
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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/GetMerkleTreeRow',
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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/GetTransactionOffsets',
            conduit__raw__pb2.TransactionOffsetsRequest.SerializeToString,
            conduit__raw__pb2.TransactionOffsetsResponse.FromString,
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
        return grpc.experimental.unary_unary(request, target, '/greet.ConduitRaw/GetBlockMetadata',
            conduit__raw__pb2.BlockMetadataRequest.SerializeToString,
            conduit__raw__pb2.BlockMetadataResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
