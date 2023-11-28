import io
import os
import grpc
import datetime
import pickle

from collections.abc import Iterable

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.level2 import Level2TypeRecord
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.facility.level2type import level2type_pb2, level2type_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level2TypeApi(object):
    """
    Level2Type Data Operation Class
    """    
    def __init__(self):
        self.stub = level2type_pb2_grpc.Level2TypeSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level2type records from database

        parameter kwargs:
            module_id: [str]
            data_type: [str]
            import_status : [int],
            page: [int]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        the_limit = get_parameter(kwargs, "limit", 100000)
        the_limit = the_limit if the_limit > 0 else 100000

        try:
            resp, _ =  self.stub.Find.with_call(level2type_pb2.FindLevel2TypeReq(
                module_id = get_parameter(kwargs, "module_id"),
                data_type = get_parameter(kwargs, "data_type"),
                import_status = get_parameter(kwargs, "import_status", 1024),
                limit = the_limit,
                page = get_parameter(kwargs, "page", 1)
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level2TypeRecord, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))


    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            data_type: [str]

        return csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Get.with_call(level2type_pb2.GetLevel2TypeReq(
                data_type = get_parameter(kwargs, "data_type")
            ),metadata = get_auth_headers())

            if not resp.record or not resp.record.data_type:
                return Result.error(message=f"data not found")  

            return Result.ok_data(data = Level2TypeRecord().from_proto_model(resp.record))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))   

    def update_import_status(self, **kwargs):
        ''' update the status of level2 type

        parameter kwargs:
            data_type: [str]
            status : [int]

        return csst_dfs_common.models.Result
        '''
        data_type = get_parameter(kwargs, "data_type")
        status = get_parameter(kwargs, "status", 0)
        try:
            resp,_ = self.stub.UpdateImportStatus.with_call(
                level2type_pb2.UpdateImportStatusReq(data_type=data_type, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))


    def write(self, **kwargs):
        ''' insert a level2type record into database

        parameter kwargs:
            data_type : [str]
            module_id : [str]
            key_column : [str]
            hdu_index : [int]
            demo_filename : [str]
            demo_file_path : [str]    
            ra_column : [str]
            dec_column : [str]

        return csst_dfs_common.models.Result
        '''   

        rec = level2type_pb2.Level2TypeRecord(
            data_type = get_parameter(kwargs, "data_type", ""),
            module_id = get_parameter(kwargs, "module_id", ""),
            key_column = get_parameter(kwargs, "key_column", ""),
            hdu_index = get_parameter(kwargs, "hdu_index", 0),
            demo_filename = get_parameter(kwargs, "demo_filename", ""),
            demo_file_path = get_parameter(kwargs, "demo_file_path", ""),            
            ra_column = get_parameter(kwargs, "ra_column", ""),            
            dec_column = get_parameter(kwargs, "dec_column", "")
        )
        def stream(rec):
            with open(rec.demo_file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield level2type_pb2.WriteLevel2TypeReq(record = rec, data = data)
        try:
            if not rec.data_type:
                return Result.error(message="data_type is blank")
            if not rec.demo_file_path:
                return Result.error(message="demo_file_path is blank")
            if not os.path.exists(rec.demo_file_path):
                return Result.error(message="the file [%s] not existed" % (rec.demo_file_path, ))
            if not rec.demo_filename:
                rec.demo_filename = os.path.basename(rec.demo_file_path)

            resp,_ = self.stub.Write.with_call(stream(rec),metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level2TypeRecord().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
