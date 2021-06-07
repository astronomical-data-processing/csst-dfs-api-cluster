import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.facility.detector import detector_pb2, detector_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class DetectorApi(object):
    def __init__(self):
        self.stub = detector_pb2_grpc.DetectorSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve detector records from database

        parameter kwargs:
            module_id: [str]
            key: [str]

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(detector_pb2.FindDetectorReq(
                module_id = get_parameter(kwargs, "module_id"),
                key = get_parameter(kwargs, "key")
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
            no : [str] 

        return csst_dfs_common.models.Result
        '''
        try:
            no = get_parameter(kwargs, "no")
            resp, _ =  self.stub.Get.with_call(detector_pb2.GetDetectorReq(
                no = no
            ),metadata = get_auth_headers())

            if not resp.record.no:
                return Result.error(message=f"no:{no} not found")  

            return Result.ok_data(data=resp.record)
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   

    def update(self, **kwargs):
        ''' update a detector by no

        parameter kwargs:
            no : [str],
            detector_name : [str],
            module_id : [str],
            filter_id : [str]

        return csst_dfs_common.models.Result
        '''
        try:
            no = get_parameter(kwargs, "no")
            result_get = self.get(no=no)
            if not result_get.success:
                return result_get

            record = detector_pb2.Detector(
                no = no,
                detector_name = get_parameter(kwargs, "detector_name", result_get.data.detector_name),
                module_id = get_parameter(kwargs, "module_id", result_get.data.module_id),
                filter_id = get_parameter(kwargs, "filter_id", result_get.data.filter_id)
            )            
            resp,_ = self.stub.Update.with_call(
                detector_pb2.UpdateDetectorReq(record=record),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def delete(self, **kwargs):
        ''' delete a detector by no

        parameter kwargs:
            no : [str]

        return csst_dfs_common.models.Result
        '''
        no = get_parameter(kwargs, "no")

        try:
            resp,_ = self.stub.Delete.with_call(
                detector_pb2.DeleteDetectorReq(no=no),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' insert a detector record into database
 
        parameter kwargs:
            no : [str],
            detector_name : [str],
            module_id : [str],
            filter_id : [str]
        return csst_dfs_common.models.Result
        '''   

        rec = detector_pb2.Detector(
            no = get_parameter(kwargs, "no"),
            detector_name = get_parameter(kwargs, "detector_name"),
            module_id = get_parameter(kwargs, "module_id"),
            filter_id = get_parameter(kwargs, "filter_id")
        )
        req = detector_pb2.WriteDetectorReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=resp.record)
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find_status(self, **kwargs):
        ''' retrieve a detector status's from database

        parameter kwargs:
            detector_no: [str]
            status_occur_time: (begin,end)
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindStatus.with_call(detector_pb2.FindStatusReq(
                detector_no = get_parameter(kwargs, "detector_no"),
                status_begin_time = get_parameter(kwargs, "status_occur_time", [None, None])[0],
                status_end_time = get_parameter(kwargs, "status_occur_time", [None, None])[1],                
                limit = get_parameter(kwargs, "limit", 0)
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=resp.records).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get_status(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            id = get_parameter(kwargs, "id")
            resp, _ =  self.stub.GetStatus.with_call(detector_pb2.GetStatusReq(
                id = id
            ),metadata = get_auth_headers())

            if resp.record is None:
                return Result.error(message=f"id:{id} not found")  

            return Result.ok_data(data=resp.record)
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   
    
    def write_status(self, **kwargs):
        ''' insert a detector status into database
 
        parameter kwargs:
            detector_no : [str],
            status : [str],
            status_time : [str]
        return csst_dfs_common.models.Result
        '''   

        rec = detector_pb2.DetectorStatus(
            id = 0,
            detector_no = get_parameter(kwargs, "detector_no"),
            status = get_parameter(kwargs, "status"),
            status_time = get_parameter(kwargs, "status_time")
        )
        req = detector_pb2.WriteStatusReq(record = rec)
        try:
            resp,_ = self.stub.WriteStatus.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=resp.record)
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))            