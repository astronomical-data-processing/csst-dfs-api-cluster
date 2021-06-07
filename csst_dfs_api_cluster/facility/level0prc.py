import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.facility.level0prc import level0prc_pb2, level0prc_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level0PrcApi(object):
    def __init__(self):
        self.stub = level0prc_pb2_grpc.Level0PrcSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level0 procedure records from database

        parameter kwargs:
            level0_id: [int]
            pipeline_id: [str]
            prc_module: [str]
            prc_status : [int]

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level0prc_pb2.FindLevel0PrcReq(
                level0_id = get_parameter(kwargs, "level0_id"),
                pipeline_id = get_parameter(kwargs, "pipeline_id"),
                prc_module = get_parameter(kwargs, "prc_module"),
                prc_status = get_parameter(kwargs, "prc_status"),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=resp.records).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            id : [int],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        id = get_parameter(kwargs, "id")
        status = get_parameter(kwargs, "status")

        try:
            resp,_ = self.stub.UpdateProcStatus.with_call(
                level0prc_pb2.UpdateProcStatusReq(id=id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' insert a level0 procedure record into database
 
        parameter kwargs:
            level0_id : [int]
            pipeline_id : [str]
            prc_module : [str]
            params_id : [str]
            prc_status : [int]
            prc_time : [str]
            file_path : [str]
        return csst_dfs_common.models.Result
        '''   

        rec = level0prc_pb2.Level0PrcRecord(
            id = 0,
            level0_id = get_parameter(kwargs, "level0_id"),
            pipeline_id = get_parameter(kwargs, "pipeline_id"),
            prc_module = get_parameter(kwargs, "prc_module"),
            params_id = get_parameter(kwargs, "params_id"),
            prc_status = get_parameter(kwargs, "prc_status"),
            prc_time = get_parameter(kwargs, "prc_time"),
            file_path = get_parameter(kwargs, "file_path")
        )
        req = level0prc_pb2.WriteLevel0PrcReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req,metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=resp.record)
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

   

