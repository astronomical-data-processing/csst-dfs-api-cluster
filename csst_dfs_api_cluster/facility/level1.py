import os
import grpc
import datetime

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.facility import Level1Record
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE
from csst_dfs_proto.facility.level1 import level1_pb2, level1_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level1DataApi(object):
    """
    Level1 Data Operation Class
    """    
    def __init__(self):
        self.stub = level1_pb2_grpc.Level1SrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level1 records from database

        parameter kwargs:
            level0_id: [str]
            module_id: [str]
            data_type: [str]
            create_time : (start, end),
            qc1_status : [int],
            prc_status : [int],
            filename: [str]
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level1_pb2.FindLevel1Req(
                level0_id = get_parameter(kwargs, "level0_id"),
                module_id = get_parameter(kwargs, "module_id"),
                data_type = get_parameter(kwargs, "data_type"),
                create_time_start = get_parameter(kwargs, "create_time", [None, None])[0],
                create_time_end = get_parameter(kwargs, "create_time", [None, None])[1],
                qc1_status = get_parameter(kwargs, "qc1_status", 1024),
                prc_status = get_parameter(kwargs, "prc_status", 1024),
                filename = get_parameter(kwargs, "filename"),
                limit = get_parameter(kwargs, "limit", 0),
                pipeline_id = get_parameter(kwargs, "pipeline_id", ""),
                detector_no = get_parameter(kwargs, "detector_no", ""),
                other_conditions = {
                    "ra_cen": str(get_parameter(kwargs, "ra_cen", '')),
                    "dec_cen": str(get_parameter(kwargs, "dec_cen", '')),
                    "radius_cen": str(get_parameter(kwargs, "radius_cen", ''))
                }
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level1Record, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def find_by_brick_ids(self, **kwargs):
        ''' retrieve level1 records by brick_ids like [1,2,3,4]

        :param kwargs: Parameter dictionary, key items support:
            brick_ids: [list]

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindByBrickIds.with_call(level1_pb2.FindByBrickIdsReq(
                brick_ids = get_parameter(kwargs, "brick_ids", [])
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level1Record, resp.records))
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def sls_find_by_qc1_status(self, **kwargs):
        ''' retrieve level1 records from database

        parameter kwargs:
            qc1_status: [str]
            limit: limits returns the number of records,default 1

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindByQc1Status.with_call(level1_pb2.FindLevel1Req(
                level0_id = None,
                data_type = None,
                create_time_start = None,
                create_time_end = None,
                qc1_status = get_parameter(kwargs, "qc1_status", -1),
                prc_status = None,
                limit = get_parameter(kwargs, "limit", 1),
                other_conditions = {"orderBy":"create_time asc", "module_id": 'SLS'}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level1Record, resp.records)).append("totalCount", resp.totalCount)
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
            resp, _ =  self.stub.Get.with_call(level1_pb2.GetLevel1Req(
                id = get_parameter(kwargs, "id"),
                level0_id = get_parameter(kwargs, "level0_id"),
                data_type = get_parameter(kwargs, "data_type") 
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"data not found")  

            return Result.ok_data(data = Level1Record().from_proto_model(resp.record))

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
                level1_pb2.UpdateProcStatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def update_qc1_status(self, **kwargs):
        ''' update the status of QC0
        
        parameter kwargs:
            id : [int],
            status : [int]
        '''        
        fits_id = get_parameter(kwargs, "id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateQc1Status.with_call(
                level1_pb2.UpdateQc1StatusReq(id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def write(self, **kwargs):
        ''' insert a level1 record into database

        parameter kwargs:
            level0_id : [str]
            data_type : [str]
            cor_sci_id : [int]
            prc_params : [str]
            filename : [str]
            file_path : [str]            
            prc_status : [int]
            prc_time : [str]
            pipeline_id : [str]
            refs: [dict]

        return csst_dfs_common.models.Result
        '''   

        rec = level1_pb2.Level1Record(
            id = 0,
            level0_id = get_parameter(kwargs, "level0_id"),
            module_id = get_parameter(kwargs, "module_id", ''),
            data_type = get_parameter(kwargs, "data_type"),
            cor_sci_id = get_parameter(kwargs, "cor_sci_id"),
            prc_params = get_parameter(kwargs, "prc_params"),
            filename = get_parameter(kwargs, "filename", ""),
            file_path = get_parameter(kwargs, "file_path", ""),
            prc_status = get_parameter(kwargs, "prc_status", -1),
            prc_time = get_parameter(kwargs, "prc_time", format_datetime(datetime.now())),
            pipeline_id = get_parameter(kwargs, "pipeline_id"),
            refs = get_parameter(kwargs, "refs", {})
        )
        def stream(rec):
            with open(rec.file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield level1_pb2.WriteLevel1Req(record = rec, data = data)
        try:
            if not rec.file_path:
                return Result.error(message="file_path is blank")
            if not os.path.exists(rec.file_path):
                return Result.error(message="the file [%s] not existed" % (rec.file_path, ))
            if not rec.filename:
                rec.filename = os.path.basename(rec.file_path)

            resp,_ = self.stub.Write.with_call(stream(rec),metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level1Record().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
