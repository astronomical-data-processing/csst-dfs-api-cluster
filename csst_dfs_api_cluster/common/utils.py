import os
from datetime import datetime
import time
import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.common.misc import misc_pb2, misc_pb2_grpc
from .service import ServiceProxy

def format_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def format_date(dt):
    return dt.strftime('%Y-%m-%d')

def format_time_ms(float_time):
    local_time = time.localtime(float_time)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (float_time - int(float_time)) * 1000
    return "%s.%03d" % (data_head, data_secs)

def get_parameter(kwargs, key, default=None):
    """ Get a specified named value for this (calling) function

    The parameter is searched for in kwargs

    :param kwargs: Parameter dictionary
    :param key: Key e.g. 'max_workers'
    :param default: Default value
    :return: result
    """

    if kwargs is None:
        return default

    value = default
    if key in kwargs.keys():
        value = kwargs[key]
    return value
    
def to_int(s, default_value = 0):
    try:
        return int(s)
    except:
        return default_value

def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]
    return inner

def get_auth_headers():
    return (("csst_dfs_app",os.getenv("CSST_DFS_APP_ID")),("csst_dfs_token",os.getenv("CSST_DFS_APP_TOKEN")),)

def get_nextId_by_prefix(prefix):
    stub = misc_pb2_grpc.MiscSrvStub(ServiceProxy().channel())
    try:
        resp,_ = stub.GetSeqId.with_call(
            misc_pb2.GetSeqIdReq(prefix=prefix),
            metadata = get_auth_headers()
        )
        return Result.ok_data(data=resp.nextId)
    except grpc.RpcError as e:
        return Result.error(message="%s:%s" % (e.code().value, e.details()))    