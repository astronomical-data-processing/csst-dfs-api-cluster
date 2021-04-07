import os

from .config.grpc_loader import GrpcLoader
from .config.configurator import Configurator
from .utils import singleton

def configOptions(opts):
    opts.address = os.getenv('CSST_DFS_CONFIG_SERVER', "localhost:9600") 

@singleton
class ClientConfigurator(object):
    def __init__(self) -> None:
        self.configurator = Configurator(GrpcLoader(configOptions))
        self.globalCfg = self.configurator.Global()
        self.gatewayCfg = self.configurator.Gateway()
    
globalConfig = ClientConfigurator()
