from pymicro.config.options import Options

class GrpcOptions(Options):
    def __init__(self):
        self.address = "localhost:9600"
        self.path = "micro"