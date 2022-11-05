import grpc

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.ifs import Level0Record
from csst_dfs_commons.models.constants import UPLOAD_CHUNK_SIZE

from csst_dfs_proto.ifs.level0 import level0_pb2, level0_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *

class Level0DataApi(object):
    def __init__(self):
        self.stub = level0_pb2_grpc.Level0SrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve level0 records from database

        parameter kwargs:
            obs_id: [str],
            detector_no: [str],
            obs_type: [str],
            object_name: [str],
            obs_time : (start, end),
            qc0_status : [int],
            prc_status : [int],
            file_name: [str],
            version: [str],
            ra: [float],
            dec: [float],
            radius: [float],
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level0_pb2.FindLevel0DataReq(
                obs_id = get_parameter(kwargs, "obs_id"),
                detector_no = get_parameter(kwargs, "detector_no"),
                obs_type = get_parameter(kwargs, "obs_type"),
                exp_time_start = get_parameter(kwargs, "obs_time", [None, None])[0],
                exp_time_end = get_parameter(kwargs, "obs_time", [None, None])[1],
                qc0_status = get_parameter(kwargs, "qc0_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                file_name = get_parameter(kwargs, "file_name"),
                object_name = get_parameter(kwargs, "object_name"),
                version = get_parameter(kwargs, "version"),
                ra = get_parameter(kwargs, "ra"),
                dec = get_parameter(kwargs, "dec"),
                radius = get_parameter(kwargs, "radius"),
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=from_proto_model_list(Level0Record, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int],
            level0_id: [str]

        return csst_dfs_common.models.Result
        '''
        try:
            id = get_parameter(kwargs, "id")
            level0_id = get_parameter(kwargs, "level0_id")
            resp, _ =  self.stub.Get.with_call(level0_pb2.GetLevel0DataReq(
                id = id,
                level0_id = level0_id
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"not found")  

            return Result.ok_data(data = Level0Record().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))   

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            id : [int],
            level0_id: [str],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        id = get_parameter(kwargs, "id")
        level0_id = get_parameter(kwargs, "level0_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateProcStatus.with_call(
                level0_pb2.UpdateProcStatusReq(
                    id=id,
                    level0_id = level0_id,
                    status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def update_qc0_status(self, **kwargs):
        ''' update the status of QC0
        
        parameter kwargs:
            id : [int],
            level0_id: [str],
            status : [int]
        '''        
        id = get_parameter(kwargs, "id")
        level0_id = get_parameter(kwargs, "level0_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateQc0Status.with_call(
                level0_pb2.UpdateQc0StatusReq( 
                    id=id,
                    level0_id = level0_id,
                    status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))

    def write(self, **kwargs):
        ''' insert a level0 data record into database
 
        parameter kwargs:
            file_path = [str]
        return: csst_dfs_common.models.Result
        '''          
        rec = level0_pb2.Level0Record(
            file_path = get_parameter(kwargs, "file_path")
        )

        def stream(rec):
            with open(rec.file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield level0_pb2.WriteLevel0Req(record = rec, data = data)

        try:
            if not rec.file_path:
                return Result.error(message="file_path is blank")
            if not os.path.exists(rec.file_path):
                return Result.error(message="the file [%s] not existed" % (rec.file_path, ))
            if not rec.filename:
                rec.filename = os.path.basename(rec.file_path)

            resp,_ = self.stub.Write.with_call(stream(rec),metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level0Record().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))        
     

