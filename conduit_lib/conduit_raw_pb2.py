# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: conduit_raw.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='conduit_raw.proto',
  package='greet',
  syntax='proto3',
  serialized_options=b'\252\002\rConduitRawAPI',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x11\x63onduit_raw.proto\x12\x05greet\"\x1b\n\x0bPingRequest\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\t\"\x1f\n\x0cPingResponse\x12\x0f\n\x07message\x18\x01 \x01(\t\"\'\n\x12\x42lockNumberRequest\x12\x11\n\tblockHash\x18\x01 \x01(\x0c\"*\n\x13\x42lockNumberResponse\x12\x13\n\x0b\x62lockNumber\x18\x01 \x01(\x04\"#\n\x0c\x42lockRequest\x12\x13\n\x0b\x62lockNumber\x18\x01 \x01(\r\"!\n\rBlockResponse\x12\x10\n\x08rawBlock\x18\x01 \x01(\x0c\"8\n\x14MerkleTreeRowRequest\x12\x11\n\tblockHash\x18\x01 \x01(\x0c\x12\r\n\x05level\x18\x02 \x01(\r\")\n\x15MerkleTreeRowResponse\x12\x10\n\x08mtreeRow\x18\x01 \x01(\x0c\".\n\x19TransactionOffsetsRequest\x12\x11\n\tblockHash\x18\x01 \x01(\x0c\"4\n\x1aTransactionOffsetsResponse\x12\x16\n\x0etxOffsetsArray\x18\x01 \x03(\x04\")\n\x14\x42lockMetadataRequest\x12\x11\n\tblockHash\x18\x01 \x01(\x0c\"/\n\x15\x42lockMetadataResponse\x12\x16\n\x0e\x62lockSizeBytes\x18\x01 \x01(\x04\x32\xb9\x03\n\nConduitRaw\x12/\n\x04Ping\x12\x12.greet.PingRequest\x1a\x13.greet.PingResponse\x12G\n\x0eGetBlockNumber\x12\x19.greet.BlockNumberRequest\x1a\x1a.greet.BlockNumberResponse\x12\x35\n\x08GetBlock\x12\x13.greet.BlockRequest\x1a\x14.greet.BlockResponse\x12M\n\x10GetMerkleTreeRow\x12\x1b.greet.MerkleTreeRowRequest\x1a\x1c.greet.MerkleTreeRowResponse\x12\\\n\x15GetTransactionOffsets\x12 .greet.TransactionOffsetsRequest\x1a!.greet.TransactionOffsetsResponse\x12M\n\x10GetBlockMetadata\x12\x1b.greet.BlockMetadataRequest\x1a\x1c.greet.BlockMetadataResponseB\x10\xaa\x02\rConduitRawAPIb\x06proto3'
)




_PINGREQUEST = _descriptor.Descriptor(
  name='PingRequest',
  full_name='greet.PingRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='data', full_name='greet.PingRequest.data', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=28,
  serialized_end=55,
)


_PINGRESPONSE = _descriptor.Descriptor(
  name='PingResponse',
  full_name='greet.PingResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='message', full_name='greet.PingResponse.message', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=57,
  serialized_end=88,
)


_BLOCKNUMBERREQUEST = _descriptor.Descriptor(
  name='BlockNumberRequest',
  full_name='greet.BlockNumberRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockHash', full_name='greet.BlockNumberRequest.blockHash', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=90,
  serialized_end=129,
)


_BLOCKNUMBERRESPONSE = _descriptor.Descriptor(
  name='BlockNumberResponse',
  full_name='greet.BlockNumberResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockNumber', full_name='greet.BlockNumberResponse.blockNumber', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=131,
  serialized_end=173,
)


_BLOCKREQUEST = _descriptor.Descriptor(
  name='BlockRequest',
  full_name='greet.BlockRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockNumber', full_name='greet.BlockRequest.blockNumber', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=175,
  serialized_end=210,
)


_BLOCKRESPONSE = _descriptor.Descriptor(
  name='BlockResponse',
  full_name='greet.BlockResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='rawBlock', full_name='greet.BlockResponse.rawBlock', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=212,
  serialized_end=245,
)


_MERKLETREEROWREQUEST = _descriptor.Descriptor(
  name='MerkleTreeRowRequest',
  full_name='greet.MerkleTreeRowRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockHash', full_name='greet.MerkleTreeRowRequest.blockHash', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='level', full_name='greet.MerkleTreeRowRequest.level', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=247,
  serialized_end=303,
)


_MERKLETREEROWRESPONSE = _descriptor.Descriptor(
  name='MerkleTreeRowResponse',
  full_name='greet.MerkleTreeRowResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='mtreeRow', full_name='greet.MerkleTreeRowResponse.mtreeRow', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=305,
  serialized_end=346,
)


_TRANSACTIONOFFSETSREQUEST = _descriptor.Descriptor(
  name='TransactionOffsetsRequest',
  full_name='greet.TransactionOffsetsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockHash', full_name='greet.TransactionOffsetsRequest.blockHash', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=348,
  serialized_end=394,
)


_TRANSACTIONOFFSETSRESPONSE = _descriptor.Descriptor(
  name='TransactionOffsetsResponse',
  full_name='greet.TransactionOffsetsResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='txOffsetsArray', full_name='greet.TransactionOffsetsResponse.txOffsetsArray', index=0,
      number=1, type=4, cpp_type=4, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=396,
  serialized_end=448,
)


_BLOCKMETADATAREQUEST = _descriptor.Descriptor(
  name='BlockMetadataRequest',
  full_name='greet.BlockMetadataRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockHash', full_name='greet.BlockMetadataRequest.blockHash', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=450,
  serialized_end=491,
)


_BLOCKMETADATARESPONSE = _descriptor.Descriptor(
  name='BlockMetadataResponse',
  full_name='greet.BlockMetadataResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='blockSizeBytes', full_name='greet.BlockMetadataResponse.blockSizeBytes', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=493,
  serialized_end=540,
)

DESCRIPTOR.message_types_by_name['PingRequest'] = _PINGREQUEST
DESCRIPTOR.message_types_by_name['PingResponse'] = _PINGRESPONSE
DESCRIPTOR.message_types_by_name['BlockNumberRequest'] = _BLOCKNUMBERREQUEST
DESCRIPTOR.message_types_by_name['BlockNumberResponse'] = _BLOCKNUMBERRESPONSE
DESCRIPTOR.message_types_by_name['BlockRequest'] = _BLOCKREQUEST
DESCRIPTOR.message_types_by_name['BlockResponse'] = _BLOCKRESPONSE
DESCRIPTOR.message_types_by_name['MerkleTreeRowRequest'] = _MERKLETREEROWREQUEST
DESCRIPTOR.message_types_by_name['MerkleTreeRowResponse'] = _MERKLETREEROWRESPONSE
DESCRIPTOR.message_types_by_name['TransactionOffsetsRequest'] = _TRANSACTIONOFFSETSREQUEST
DESCRIPTOR.message_types_by_name['TransactionOffsetsResponse'] = _TRANSACTIONOFFSETSRESPONSE
DESCRIPTOR.message_types_by_name['BlockMetadataRequest'] = _BLOCKMETADATAREQUEST
DESCRIPTOR.message_types_by_name['BlockMetadataResponse'] = _BLOCKMETADATARESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

PingRequest = _reflection.GeneratedProtocolMessageType('PingRequest', (_message.Message,), {
  'DESCRIPTOR' : _PINGREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.PingRequest)
  })
_sym_db.RegisterMessage(PingRequest)

PingResponse = _reflection.GeneratedProtocolMessageType('PingResponse', (_message.Message,), {
  'DESCRIPTOR' : _PINGRESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.PingResponse)
  })
_sym_db.RegisterMessage(PingResponse)

BlockNumberRequest = _reflection.GeneratedProtocolMessageType('BlockNumberRequest', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKNUMBERREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockNumberRequest)
  })
_sym_db.RegisterMessage(BlockNumberRequest)

BlockNumberResponse = _reflection.GeneratedProtocolMessageType('BlockNumberResponse', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKNUMBERRESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockNumberResponse)
  })
_sym_db.RegisterMessage(BlockNumberResponse)

BlockRequest = _reflection.GeneratedProtocolMessageType('BlockRequest', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockRequest)
  })
_sym_db.RegisterMessage(BlockRequest)

BlockResponse = _reflection.GeneratedProtocolMessageType('BlockResponse', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKRESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockResponse)
  })
_sym_db.RegisterMessage(BlockResponse)

MerkleTreeRowRequest = _reflection.GeneratedProtocolMessageType('MerkleTreeRowRequest', (_message.Message,), {
  'DESCRIPTOR' : _MERKLETREEROWREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.MerkleTreeRowRequest)
  })
_sym_db.RegisterMessage(MerkleTreeRowRequest)

MerkleTreeRowResponse = _reflection.GeneratedProtocolMessageType('MerkleTreeRowResponse', (_message.Message,), {
  'DESCRIPTOR' : _MERKLETREEROWRESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.MerkleTreeRowResponse)
  })
_sym_db.RegisterMessage(MerkleTreeRowResponse)

TransactionOffsetsRequest = _reflection.GeneratedProtocolMessageType('TransactionOffsetsRequest', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONOFFSETSREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.TransactionOffsetsRequest)
  })
_sym_db.RegisterMessage(TransactionOffsetsRequest)

TransactionOffsetsResponse = _reflection.GeneratedProtocolMessageType('TransactionOffsetsResponse', (_message.Message,), {
  'DESCRIPTOR' : _TRANSACTIONOFFSETSRESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.TransactionOffsetsResponse)
  })
_sym_db.RegisterMessage(TransactionOffsetsResponse)

BlockMetadataRequest = _reflection.GeneratedProtocolMessageType('BlockMetadataRequest', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKMETADATAREQUEST,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockMetadataRequest)
  })
_sym_db.RegisterMessage(BlockMetadataRequest)

BlockMetadataResponse = _reflection.GeneratedProtocolMessageType('BlockMetadataResponse', (_message.Message,), {
  'DESCRIPTOR' : _BLOCKMETADATARESPONSE,
  '__module__' : 'conduit_raw_pb2'
  # @@protoc_insertion_point(class_scope:greet.BlockMetadataResponse)
  })
_sym_db.RegisterMessage(BlockMetadataResponse)


DESCRIPTOR._options = None

_CONDUITRAW = _descriptor.ServiceDescriptor(
  name='ConduitRaw',
  full_name='greet.ConduitRaw',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=543,
  serialized_end=984,
  methods=[
  _descriptor.MethodDescriptor(
    name='Ping',
    full_name='greet.ConduitRaw.Ping',
    index=0,
    containing_service=None,
    input_type=_PINGREQUEST,
    output_type=_PINGRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetBlockNumber',
    full_name='greet.ConduitRaw.GetBlockNumber',
    index=1,
    containing_service=None,
    input_type=_BLOCKNUMBERREQUEST,
    output_type=_BLOCKNUMBERRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetBlock',
    full_name='greet.ConduitRaw.GetBlock',
    index=2,
    containing_service=None,
    input_type=_BLOCKREQUEST,
    output_type=_BLOCKRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetMerkleTreeRow',
    full_name='greet.ConduitRaw.GetMerkleTreeRow',
    index=3,
    containing_service=None,
    input_type=_MERKLETREEROWREQUEST,
    output_type=_MERKLETREEROWRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetTransactionOffsets',
    full_name='greet.ConduitRaw.GetTransactionOffsets',
    index=4,
    containing_service=None,
    input_type=_TRANSACTIONOFFSETSREQUEST,
    output_type=_TRANSACTIONOFFSETSRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='GetBlockMetadata',
    full_name='greet.ConduitRaw.GetBlockMetadata',
    index=5,
    containing_service=None,
    input_type=_BLOCKMETADATAREQUEST,
    output_type=_BLOCKMETADATARESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_CONDUITRAW)

DESCRIPTOR.services_by_name['ConduitRaw'] = _CONDUITRAW

# @@protoc_insertion_point(module_scope)