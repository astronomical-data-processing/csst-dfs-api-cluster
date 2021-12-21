import grpc
import datetime

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.msc import MSCLevel2CatalogRecord
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.msc.level2catalog import level2catalog_pb2, level2catalog_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level2CatalogApi(object):
    """
    Level2Catalog Data Operation Class
    """    
    def __init__(self):
        self.stub = level2catalog_pb2_grpc.Level2CatalogSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level2catalog records from database

        parameter kwargs:
            obs_id: [str]
            detector_no: [str]
            ra:  [float] in deg
            dec: [float] in deg
            radius:  [float] in deg   
            min_mag: [float]
            max_mag: [float]
            obs_time: (start, end),
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level2catalog_pb2.FindLevel2CatalogReq(
                obs_id = get_parameter(kwargs, "obs_id"),
                detector_no = get_parameter(kwargs, "detector_no"),
                obs_time_start = get_parameter(kwargs, "obs_time", [None, None])[0],
                obs_time_end = get_parameter(kwargs, "obs_time", [None, None])[1],
                ra = get_parameter(kwargs, "ra"),
                dec = get_parameter(kwargs, "dec"),
                radius = get_parameter(kwargs, "radius"),
                minMag = get_parameter(kwargs, "min_mag"),
                maxMag = get_parameter(kwargs, "max_mag"),
                limit = get_parameter(kwargs, "limit", 0)
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(MSCLevel2CatalogRecord, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, records):
        ''' insert a level2catalog records into database
 
        parameter kwargs:
            records : list[str]
         
        return csst_dfs_common.models.Result
        '''   
        try:
            
            resp,_ = self.stub.Write.with_call(level2catalog_pb2.WriteLevel2CatalogReq(records = records),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))
