import dataclasses

@dataclasses.dataclass
class Server:
    name: str=""
    version: str=""
    address: str=""
    port: int=-1

@dataclasses.dataclass
class Global:
    fitsFileRootDir: str=""
    fileExternalPrefix: str=""

@dataclasses.dataclass
class Gateway:
    enabled: bool=True
    url: str=""

