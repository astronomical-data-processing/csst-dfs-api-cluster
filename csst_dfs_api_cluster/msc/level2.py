import os
import grpc
import datetime

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.msc import Level2Record,Level2CatalogRecord
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.msc.level2 import level2_pb2, level2_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level2DataApi(object):
    """
    Level2 Data Operation Class
    """    
    def __init__(self):
        self.stub = level2_pb2_grpc.Level2SrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level2 records from database

        parameter kwargs:
            level0_id: [str]
            data_type: [str]
            create_time : (start, end),
            qc2_status : [int],
            prc_status : [int],
            filename: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level2_pb2.FindLevel2Req(
                level1_id = get_parameter(kwargs, "level1_id"),
                data_type = get_parameter(kwargs, "data_type"),
                create_time_start = get_parameter(kwargs, "create_time", [None, None])[0],
                create_time_end = get_parameter(kwargs, "create_time", [None, None])[1],
                qc2_status = get_parameter(kwargs, "qc2_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                filename = get_parameter(kwargs, "filename"),
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level2Record, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def catalog_query(self, **kwargs):
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
            resp, _ =  self.stub.FindCatalog.with_call(level2_pb2.FindLevel2CatalogReq(
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

    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Get.with_call(level2_pb2.GetLevel2Req(
                id = get_parameter(kwargs, "id")
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"data not found")  

            return Result.ok_data(data = Level2Record().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            id : [int],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        fits_id = get_parameter(kwargs, "id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateProcStatus.with_call(
                level2_pb2.UpdateProcStatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_qc2_status(self, **kwargs):
        ''' update the status of QC0
        
        parameter kwargs:
            id : [int],
            status : [int]
        '''        
        fits_id = get_parameter(kwargs, "id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateQc2Status.with_call(
                level2_pb2.UpdateQc2StatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' insert a level2 record into database
 
        parameter kwargs:
            level1_id : [int]
            data_type : [str]
            filename : [str]
            file_path : [str]            
            prc_status : [int]
            prc_time : [str]

        return csst_dfs_common.models.Result
        '''   

        rec = level2_pb2.Level2Record(
            id = 0,
            level1_id = get_parameter(kwargs, "level1_id"),
            data_type = get_parameter(kwargs, "data_type"),
            filename = get_parameter(kwargs, "filename", ""),
            file_path = get_parameter(kwargs, "file_path", ""),
            prc_status = get_parameter(kwargs, "prc_status", -1),
            prc_time = get_parameter(kwargs, "prc_time", format_datetime(datetime.now()))
        )
        def stream(rec):
            with open(rec.file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield level2_pb2.WriteLevel2Req(record = rec, data = data)
        try:
            if not rec.file_path:
                return Result.error(message="file_path is blank")
            if not os.path.exists(rec.file_path):
                return Result.error(message="the file [%s] not existed" % (rec.file_path, ))
            if not rec.filename:
                rec.filename = os.path.basename(rec.file_path)

            resp,_ = self.stub.Write.with_call(stream(rec),metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level2Record().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))
