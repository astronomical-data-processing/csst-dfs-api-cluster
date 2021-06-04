import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.facility.level0 import level0_pb2, level0_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level0DataApi(object):
    def __init__(self):
        self.stub = level0_pb2_grpc.Level0SrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level0 records from database

        parameter kwargs:
            obs_id: [int]
            detector_no: [str]
            obs_type: [str]
            exp_time : (start, end),
            qc0_status : [int],
            prc_status : [int],
            file_name: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level0_pb2.FindLevel0DataReq(
                obs_id = get_parameter(kwargs, "obs_id"),
                detector_no = get_parameter(kwargs, "detector_no"),
                obs_type = get_parameter(kwargs, "obs_type"),
                exp_time_start = get_parameter(kwargs, "exp_time", [None, None])[0],
                exp_time_end = get_parameter(kwargs, "exp_time", [None, None])[1],
                qc0_status = get_parameter(kwargs, "qc0_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                file_name = get_parameter(kwargs, "file_name"),
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=resp.records).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = resp.message)

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            fits_id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            fits_id = get_parameter(kwargs, "fits_id")
            resp, _ =  self.stub.Get.with_call(level0_pb2.GetLevel0DataReq(
                id = fits_id
            ),metadata = get_auth_headers())

            if resp.record is None:
                return Result.error(message=f"fits_id:{fits_id} not found")  

            return Result.ok_data(data=resp.record)
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   

    def read(self, **kwargs):
        ''' yield bytes of fits file

        parameter kwargs:
            fits_id = [int],
            file_path = [str], 
            chunk_size = [int] default 20480

        yield bytes of fits file
        '''
        try:
            streams = self.stub.Read.with_call(level0_pb2.ReadLevel0DatasReq(
                id = get_parameter(kwargs, "fits_id"),
                file_path = get_parameter(kwargs, "file_path"),
                chunk_size = get_parameter(kwargs, "chunk_size", 20480)
            ),metadata = get_auth_headers())

            for stream in streams:
                yield stream.data

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            fits_id : [int],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateProcStatus.with_call(
                level0_pb2.UpdateProcStatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_qc0_status(self, **kwargs):
        ''' update the status of QC0
        
        parameter kwargs:
            fits_id : [int],
            status : [int]
        '''        
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateQc0Status.with_call(
                level0_pb2.UpdateQc0StatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))



