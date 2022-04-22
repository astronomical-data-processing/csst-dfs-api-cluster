import grpc

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.facility import Brick, FindObsStatus, BrickLevel1

from csst_dfs_proto.facility.brick import brick_pb2, brick_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import UPLOAD_CHUNK_SIZE

class BrickApi(object):
    """
    Brick Operation Class
    """    
    def __init__(self):
        self.stub = brick_pb2_grpc.BrickSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' find brick records

        :param kwargs:
            limit: limits returns the number of records,default 0:no-limit

        :returns: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(brick_pb2.FindBrickReq(
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Brick, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get(self, **kwargs):
        '''  fetch a record from database

        :param kwargs:
            id : [int]

        :returns: csst_dfs_common.models.Result
        '''
        try:
            brick_id = get_parameter(kwargs, "id", -1)
            resp, _ =  self.stub.Get.with_call(brick_pb2.GetBrickReq(
                id = brick_id
            ),metadata = get_auth_headers())

            if resp.brick is None or resp.brick.id == -1:
                return Result.error(message=f"{brick_id} not found")  

            return Result.ok_data(data=Brick().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))        

    def write(self, **kwargs):
        ''' insert a brickal record into database

        :param kwargs: Parameter dictionary, key items support:
            ra = [float],
            dec = [float],
            boundingbox = [str]
        
        :returns: csst_dfs_common.models.Result
        '''  
        rec = brick_pb2.Brick(
            id = get_parameter(kwargs, "id", 0),
            ra = get_parameter(kwargs, "ra", 0.0),
            dec = get_parameter(kwargs, "dec", 0.0),
            boundingbox = get_parameter(kwargs, "boundingbox", "")
        )
        req = brick_pb2.WriteBrickReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req,metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Brick().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find_obs_status(self, **kwargs):
        ''' find observation status of bricks

        :param kwargs:
            brick_id = [int],
            band = [string]

        :returns: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindObsStatus.with_call(brick_pb2.FindObsStatusReq(
                brick_id = get_parameter(kwargs, "brick_id", -1),
                band = get_parameter(kwargs, "band", "")
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(FindObsStatus, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find_level1_data(self, **kwargs):
        ''' find level1 data

        :param kwargs: Parameter dictionary, support:
            brick_id = [int]\n
            level1_id = [int]\n
            module = [str]

        :returns: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindLevel1.with_call(brick_pb2.FindLevel1Req(
                brick_id = get_parameter(kwargs, "brick_id", -1),
                level1_id = get_parameter(kwargs, "level1_id", 0),
                module = get_parameter(kwargs, "limit", "")
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(BrickLevel1, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))
