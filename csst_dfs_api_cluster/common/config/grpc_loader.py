
import grpc

from .grpc_options import GrpcOptions
from pymicro.config.grpc.proto import grpc_pb2, grpc_pb2_grpc
from .changeset import ChangeSet

class GrpcLoader(object):

    def __init__(self, *args, **kwargs):
        self.options = GrpcOptions()
        for o in args:
            o(self.options)
        
        self._connect()

    def Read(self):
        request = grpc_pb2.ReadRequest(path = self.options.path)
        response = self.stub.Read(request)
        return self._to_changset(response.change_set)

    def _connect(self):
        self.channel = grpc.insecure_channel(self.options.address)
        self.stub = grpc_pb2_grpc.SourceStub(self.channel)

    def _to_changset(self, cs):
        changeset = ChangeSet()
        changeset.data = cs.data
        changeset.checksum = cs.checksum
        changeset.source = cs.source
        changeset.format = cs.format
        changeset.timestamp = cs.timestamp
        return changeset


