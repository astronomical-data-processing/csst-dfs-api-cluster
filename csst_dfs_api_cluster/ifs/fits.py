import os
import grpc

from csst_dfs_commons.models import Result
from csst_dfs_proto.ifs.fits import fits_pb2, fits_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import UPLOAD_CHUNK_SIZE

class FitsApi(object):
    def __init__(self, sub_system = "ifs"):
        self.sub_system = sub_system
        self.stub = fits_pb2_grpc.FitsSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        '''
        parameter kwargs:
            obs_time = [int],
            file_name = [str],
            exp_time = (start, end),
            ccd_num = [int],
            qc0_status = [int],
            prc_status = [int],
            limit = [int]

        return list of raw records
        '''
        try:
            resp, _ =  self.stub.Find.with_call(fits_pb2.FindRawFitsReq(
                obs_time = get_parameter(kwargs, "obs_time"),
                file_name = get_parameter(kwargs, "file_name"),
                exp_time_start = get_parameter(kwargs, "exp_time", [None, None])[0],
                exp_time_end = get_parameter(kwargs, "exp_time", [None, None])[1],
                ccd_num = get_parameter(kwargs, "ccd_num"),
                qc0_status = get_parameter(kwargs, "qc0_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data=resp.rawFits).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = resp.message)

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get(self, **kwargs):
        '''  query database, return a record as dict

        parameter kwargs:
            fits_id = [int] 

        return dict or None
        '''

        try:
            resp, _ =  self.stub.Get.with_call(fits_pb2.GetRawFitsReq(
                fits_id = get_parameter(kwargs, "fits_id")
            ),metadata = get_auth_headers())

            return Result.ok_data(data=resp)
           
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
            streams = self.stub.Read.with_call(fits_pb2.ReadRawFitsReq(
                fits_id = get_parameter(kwargs, "fits_id"),
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
            fits_id = [int],
            status = [int]
        '''
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status"),
        try:
            resp,_ = self.stub.update_qc0_status.with_call(
                fits_pb2.UpdateProcStatusReq(fits_id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            return Result.ok_data(data=resp)
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_qc0_status(self, **kwargs):
        ''' update the status of reduction
        
        parameter kwargs:
            fits_id = [int],
            status = [int]
        '''        
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status"),
        try:
            resp,_ = self.stub.update_qc0_status.with_call(
                fits_pb2.UpdateQc0StatusReq(fits_id=fits_id, status=status),
                metadata = get_auth_headers()
            )
            return Result.ok_data(data=resp)
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' copy a local file to file storage, then reduce the header of fits file and insert a record into database
 
        parameter kwargs:
            file_path = [str]
        '''   
        file_path = get_parameter(kwargs, "file_path")

        if os.path.exists(file_path):
            raise Exception("%s file not found" % (file_path))

        def stream(v_file_path):
            v_file_name = os.path.basename(v_file_path)
            with open(v_file_path, 'rb') as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    yield fits_pb2.WriteRawFitsReq(
                                    file_name=v_file_name, 
                                    fitsData=data)
        try:
            resp,_ = self.stub.Write.with_call(stream(file_path),
                    metadata = get_auth_headers())
            return Result.ok_data(data=resp)
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

   

