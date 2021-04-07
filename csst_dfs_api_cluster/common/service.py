import grpc
import random

class _Service:
    def __init__(self, data):
        self._data = data

    @property
    def name(self):
        raise NotImplementedError()

    @property
    def version(self):
        raise NotImplementedError()

    @property
    def metadata(self):
        raise NotImplementedError()

    @property
    def id(self):
        raise NotImplementedError()

    @property
    def address(self):
        raise NotImplementedError()

    @property
    def port(self):
        raise NotImplementedError()

    @property
    def node_metadata(self):
        raise NotImplementedError()

    def __str__(self):
        return '{}:{}'.format(self.name, self.address)
        
class Service(_Service):

    def __init__(self, data):
        self._data = data

    @property
    def name(self):
        return self._data['name']

    @property
    def version(self):
        return self._data['version']

    @property
    def metadata(self):
        return self._data['metadata']

    @property
    def _node(self):
        return random.choice(self._data['nodes'])

    @property
    def id(self):
        return self._node['id']

    @property
    def address(self):
        return self._node['address']

    @property
    def node_metadata(self):
        return self._node['metadata']


class ServiceProxy:
    def __init__(self, gatewayCfg = None):
        self.gatewayCfg = gatewayCfg

    def insecure(self, stubCls):
        channel = grpc.insecure_channel(self.gatewayCfg.url)
        return stubCls(channel)