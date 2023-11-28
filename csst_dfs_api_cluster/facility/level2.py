import io
import os
import grpc
import datetime
import pickle

from collections.abc import Iterable

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.level2 import Level2Record
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.facility.level2 import level2_pb2, level2_pb2_grpc

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
            level1_id: [int]
            module_id: [str]
            brick_id: [int]            
            data_type: [str]
            create_time : (start, end),
            qc2_status : [int],
            prc_status : [int],
            import_status : [int],
            filename: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level2_pb2.FindLevel2Req(
                level0_id = get_parameter(kwargs, "level0_id"),
                level1_id = get_parameter(kwargs, "level1_id"),
                module_id = get_parameter(kwargs, "module_id"),
                brick_id = get_parameter(kwargs, "brick_id"),
                data_type = get_parameter(kwargs, "data_type"),
                create_time_start = get_parameter(kwargs, "create_time", [None, None])[0],
                create_time_end = get_parameter(kwargs, "create_time", [None, None])[1],
                qc2_status = get_parameter(kwargs, "qc2_status", 1024),
                prc_status = get_parameter(kwargs, "prc_status", 1024),
                import_status = get_parameter(kwargs, "import_status", 1024),
                filename = get_parameter(kwargs, "filename"),
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level2Record, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def catalog_query(self, **kwargs):
        ''' retrieve level2catalog records from database

        parameter kwargs:
            sql: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            datas = io.BytesIO()
            totalCount = 0         
            
            resps =  self.stub.FindCatalog(level2_pb2.FindLevel2CatalogReq(
                sql = get_parameter(kwargs, "sql", None),
                limit = get_parameter(kwargs, "limit", 0)
            ),metadata = get_auth_headers())
            
            for resp in resps:
                if resp.success:
                    datas.write(resp.records)
                    totalCount = resp.totalCount
                else:
                    return Result.error(message = str(resp.error.detail))
            datas.flush()
            records = pickle.loads(datas.getvalue())
            return Result.ok_data(data = records[0]).append("totalCount", totalCount).append("columns", records[1])
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def find_existed_brick_ids(self, **kwargs):
        ''' retrieve existed brick_ids in a single exposure catalog

        parameter kwargs:
            data_type: [str]
        return: csst_dfs_common.models.Result
        '''
        try:
            resp =  self.stub.FindExistedBricks(level2_pb2.FindExistedBricksReq(
                data_type = get_parameter(kwargs, "data_type")
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = resp.brick_ids)
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
            resp, _ =  self.stub.Get.with_call(level2_pb2.GetLevel2Req(
                id = get_parameter(kwargs, "id")
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"data not found")  

            return Result.ok_data(data = Level2Record().from_proto_model(resp.record))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))   

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
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

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
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def write(self, **kwargs):
        ''' insert a level2 record into database

        parameter kwargs:
            level1_id : [int]
            brick_id : [int]
            module_id : [str]
            object_name: [str]
            data_type : [str]
            filename : [str]
            file_path : [str]            
            prc_status : [int]
            prc_time : [str]
            pipeline_id : [str]

        return csst_dfs_common.models.Result
        '''   

        rec = level2_pb2.Level2Record(
            id = 0,
            level1_id = get_parameter(kwargs, "level1_id", 0),
            brick_id = get_parameter(kwargs, "brick_id", 0),
            module_id = get_parameter(kwargs, "module_id", ""),
            data_type = get_parameter(kwargs, "data_type", ""),
            object_name = get_parameter(kwargs, "object_name", ""),
            filename = get_parameter(kwargs, "filename", ""),
            file_path = get_parameter(kwargs, "file_path", ""),
            qc2_status = get_parameter(kwargs, "qc2_status", 0),
            prc_status = get_parameter(kwargs, "prc_status", 0),
            prc_time = get_parameter(kwargs, "prc_time", format_datetime(datetime.now())),
            pipeline_id = get_parameter(kwargs, "pipeline_id", "")
        )
        def stream(rec):
            with open(rec.file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield level2_pb2.WriteLevel2Req(record = rec, data = data)
        try:
            if not rec.module_id:
                return Result.error(message="module_id is blank")
            if not rec.data_type:
                return Result.error(message="data_type is blank")
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
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
