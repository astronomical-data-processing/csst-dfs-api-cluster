import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.facility.calmerge import calmerge_pb2, calmerge_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class CalMergeApi(object):
    def __init__(self):
        self.stub = calmerge_pb2_grpc.CalMergeSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve calibration merge records from database

        parameter kwargs:
            detector_no: [str]
            ref_type: [str]
            obs_time: (start,end)
            qc1_status : [int]
            prc_status : [int]
            file_name: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(calmerge_pb2.FindCalMergeReq(
                detector_no = get_parameter(kwargs, "detector_no"),
                ref_type = get_parameter(kwargs, "ref_type"),
                exp_time_start = get_parameter(kwargs, "obs_time", [None, None])[0],
                exp_time_end = get_parameter(kwargs, "obs_time", [None, None])[1],                
                qc1_status = get_parameter(kwargs, "qc1_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                file_name = get_parameter(kwargs, "file_name"),
                limit = get_parameter(kwargs, "limit"),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=resp.records).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            id = get_parameter(kwargs, "id")
            resp, _ =  self.stub.Get.with_call(calmerge_pb2.GetCalMergeReq(
                id = id
            ),metadata = get_auth_headers())

            if resp.record is None:
                return Result.error(message=f"id:{id} not found")  

            return Result.ok_data(data=resp.record)
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   

    def update_qc1_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            id : [int],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        id = get_parameter(kwargs, "id")
        status = get_parameter(kwargs, "status")

        try:
            resp,_ = self.stub.UpdateQc1Status.with_call(
                calmerge_pb2.UpdateQc1StatusReq(id=id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
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
                calmerge_pb2.UpdateProcStatusReq(id=id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' insert a calibration merge record into database
 
        parameter kwargs:
            id : [int]
            detector_no : [str]
            ref_type : [str]
            obs_time : [str]
            exp_time : [float]
            prc_status : [int]
            prc_time : [str]
            filename : [str]
            file_path : [str]
            level0_ids : [list]
        return csst_dfs_common.models.Result
        '''   

        rec = calmerge_pb2.CalMergeRecord(
            id = 0,
            detector_no = get_parameter(kwargs, "detector_no"),
            ref_type = get_parameter(kwargs, "ref_type"),
            obs_time = get_parameter(kwargs, "obs_time"),
            exp_time = get_parameter(kwargs, "exp_time"),
            filename = get_parameter(kwargs, "filename"),
            file_path = get_parameter(kwargs, "file_path"),
            prc_status = get_parameter(kwargs, "prc_status"),
            prc_time = get_parameter(kwargs, "prc_time"),
            level0_ids = get_parameter(kwargs, "level0_ids", [])
        )
        req = calmerge_pb2.WriteCalMergeReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req,metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=resp.record)
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

