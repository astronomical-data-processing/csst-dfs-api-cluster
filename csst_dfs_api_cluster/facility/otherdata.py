import os
import grpc
import datetime

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.facility import OtherDataRecord
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.facility.otherdata import otherdata_pb2, otherdata_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class OtherDataApi(object):
    """
    OtherData Data Operation Class
    """    
    def __init__(self):
        self.stub = otherdata_pb2_grpc.OtherDataSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve otherdata records from database

        parameter kwargs:
            obs_id: [str]
            detector_no: [str]
            module_id: [str]
            file_type: [str]
            filename: [str]
            create_time : (start, end)
            pipeline_id : [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(otherdata_pb2.FindOtherDataReq(
                obs_id = get_parameter(kwargs, "obs_id"),
                detector_no = get_parameter(kwargs, "detector_no", ""),
                module_id = get_parameter(kwargs, "module_id"),
                file_type = get_parameter(kwargs, "file_type"),
                create_time_start = get_parameter(kwargs, "create_time", [None, None])[0],
                create_time_end = get_parameter(kwargs, "create_time", [None, None])[1],
                pipeline_id = get_parameter(kwargs, "pipeline_id", ""),
                filename = get_parameter(kwargs, "filename"),
                limit = get_parameter(kwargs, "limit", 0)
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(OtherDataRecord, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))


    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Get.with_call(otherdata_pb2.GetOtherDataReq(
                id = get_parameter(kwargs, "id")
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"data not found")  

            return Result.ok_data(data = OtherDataRecord().from_proto_model(resp.record))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))   


    def write(self, **kwargs):
        ''' insert a otherdata record into database

        parameter kwargs:
            obs_id: [str]
            detector_no : [str]
            module_id : [str]
            file_type : [str]
            filename : [str]
            file_path : [str]            
            pipeline_id : [str]

        return csst_dfs_common.models.Result
        '''   

        rec = otherdata_pb2.OtherDataRecord(
            id = 0,
            obs_id = get_parameter(kwargs, "obs_id"),
            module_id = get_parameter(kwargs, "module_id", ''),
            file_type = get_parameter(kwargs, "file_type"),
            detector_no = get_parameter(kwargs, "detector_no"),
            filename = get_parameter(kwargs, "filename", ""),
            file_path = get_parameter(kwargs, "file_path", ""),
            pipeline_id = get_parameter(kwargs, "pipeline_id")
        )
        def stream(rec):
            with open(rec.file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield otherdata_pb2.WriteOtherDataReq(record = rec, data = data)
        try:
            if not rec.file_path:
                return Result.error(message="file_path is blank")
            if not os.path.exists(rec.file_path):
                return Result.error(message="the file [%s] not existed" % (rec.file_path, ))
            if not rec.filename:
                rec.filename = os.path.basename(rec.file_path)

            resp,_ = self.stub.Write.with_call(stream(rec),metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=OtherDataRecord().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
