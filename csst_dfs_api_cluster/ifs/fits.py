import os
import grpc

from csst_dfs_proto.ifs.fits import fits_pb2, fits_pb2_grpc

from ..common.client_config import ClientConfigurator
from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import UPLOAD_CHUNK_SIZE

class FitsApi(object):
    def __init__(self, sub_system = "ifs"):
        self.sub_system = sub_system
        self.proxy = ServiceProxy(ClientConfigurator().gatewayCfg)
        self.stub = self.proxy.insecure(fits_pb2_grpc.FitsSrvStub)

    def find(self, **kwargs):
        '''
        parameter kwargs:
            obs_time = [int],
            file_name = [str],
            exp_time = (start, end),
            ccd_num = [int],
            qc0_status = [int],
            prc_status = [int]

        return list of raw records
        '''
        return self.stub.Find(fits_pb2.FindRawFitsReq(
            obs_time = get_parameter(kwargs, "obs_time"),
            file_name = get_parameter(kwargs, "file_name"),
            exp_time_start = get_parameter(kwargs, "exp_time", [None, None])[0],
            exp_time_end = get_parameter(kwargs, "exp_time", [None, None])[1],
            ccd_num = get_parameter(kwargs, "ccd_num"),
            qc0_status = get_parameter(kwargs, "qc0_status"),
            prc_status = get_parameter(kwargs, "prc_status"),
            other_conditions = {"test":"cnlab.test"}
        ))

    def get(self, **kwargs):
        '''  query database, return a record as dict

        parameter kwargs:
            fits_id = [int] 

        return dict or None
        '''
        return self.stub.Get(fits_pb2.GetRawFitsReq(
            fits_id = get_parameter(kwargs, "fits_id")
        ))

    def read(self, **kwargs):
        ''' yield bytes of fits file

        parameter kwargs:
            fits_id = [int],
            file_path = [str], 
            chunk_size = [int] default 20480

        yield bytes of fits file
        '''
        try:
            streams = self.stub.Read(fits_pb2.ReadRawFitsReq(
                fits_id = get_parameter(kwargs, "fits_id"),
                file_path = get_parameter(kwargs, "file_path"),
                chunk_size = get_parameter(kwargs, "chunk_size", 20480)
            ))

            for stream in streams:
                yield stream.data

        except Exception as e:
            print(e)

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            fits_id = [int],
            status = [int]
        '''
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status"),
        try:
            return self.stub.update_qc0_status((fits_pb2.UpdateProcStatusReq(fits_id=fits_id, status=status)))

        except grpc.RpcError as identifier:
            print(identifier)    

    def update_qc0_status(self, **kwargs):
        ''' update the status of reduction
        
        parameter kwargs:
            fits_id = [int],
            status = [int]
        '''        
        fits_id = get_parameter(kwargs, "fits_id")
        status = get_parameter(kwargs, "status"),
        try:
            return self.stub.update_qc0_status((fits_pb2.UpdateQc0StatusReq(fits_id=fits_id, status=status)))

        except grpc.RpcError as identifier:
            print(identifier)         


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

        return self.stub.Write(stream(file_path))
    

   

