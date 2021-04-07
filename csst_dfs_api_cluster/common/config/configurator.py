import orjson
from .model import Server as ServerCfg, Global as GlobalCfg, Gateway as GatewayCfg

class Configurator(object):
    def __init__(self, loader):
        self.loader = loader
        change_set = self.loader.Read()
        self.config = orjson.loads(change_set.data)
    
    def Server(self, srv_name):
        return self.Any(srv_name, ServerCfg)

    def Gateway(self, path = "gateway"):
        return self.Any(path, GatewayCfg)

    def Global(self, path = "global"):
        return self.Any(path, GlobalCfg)

    def Any(self, path, ref_object):
        paths = path.split(".")
        cfg = self.config
        for p in paths:
            cfg = cfg[p]
        if isinstance(cfg, dict):
            return ref_object(**cfg)
        else:
            return ref_object(cfg)            

        




    