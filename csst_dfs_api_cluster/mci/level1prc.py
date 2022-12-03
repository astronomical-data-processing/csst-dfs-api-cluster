import grpc

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.ifs import Level1PrcRecord

from csst_dfs_proto.ifs.level1prc import level1prc_pb2, level1prc_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level1PrcApi(object):
    def __init__(self):
        self.stub = level1prc_pb2_grpc.Level1PrcSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level1 procedure records from database

        parameter kwargs:
            level1_id: [str]
            pipeline_id: [str]
            prc_module: [str]
            prc_status : [int]

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level1prc_pb2.FindLevel1PrcReq(
                level1_id = get_parameter(kwargs, "level1_id"),
                pipeline_id = get_parameter(kwargs, "pipeline_id"),
                prc_module = get_parameter(kwargs, "prc_module"),
                prc_status = get_parameter(kwargs, "prc_status"),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Level1PrcRecord, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

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
                level1prc_pb2.UpdateProcStatusReq(id=id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def write(self, **kwargs):
        ''' insert a level1 procedure record into database
 
        parameter kwargs:
            level1_id : [int]
            pipeline_id : [str]
            prc_module : [str]
            params_file_path : [str]
            prc_status : [int]
            prc_time : [str]
            result_file_path : [str]
        return csst_dfs_common.models.Result
        '''   

        rec = level1prc_pb2.Level1PrcRecord(
            id = 0,
            level1_id = get_parameter(kwargs, "level1_id"),
            pipeline_id = get_parameter(kwargs, "pipeline_id"),
            prc_module = get_parameter(kwargs, "prc_module"),
            params_file_path = get_parameter(kwargs, "params_file_path"),
            prc_status = get_parameter(kwargs, "prc_status", -1),
            prc_time = get_parameter(kwargs, "prc_time"),
            result_file_path = get_parameter(kwargs, "result_file_path")
        )
        req = level1prc_pb2.WriteLevel1PrcReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req,metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data = Level1PrcRecord().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

   

