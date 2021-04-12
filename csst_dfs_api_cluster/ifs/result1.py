
import os
import grpc

from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import *
import grpc

class Result1Api(object):
    def __init__(self, sub_system = "ifs"):
        self.sub_system = sub_system


