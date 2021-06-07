import grpc

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.facility import Observation

from csst_dfs_proto.facility.observation import observation_pb2, observation_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import UPLOAD_CHUNK_SIZE

class ObservationApi(object):
    """
    Observation Operation Class
    """    
    def __init__(self):
        self.stub = observation_pb2_grpc.ObservationSrvStub(ServiceProxy().channel())

    def find(self, **kwargs):
        ''' retrieve exposure records from database

        parameter kwargs:
            module_id: [str]
            obs_type: [str]
            obs_time : (start, end),
            qc0_status : [int],
            prc_status : [int],
            limit: limits returns the number of records,default 0:no-limit

        return: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(observation_pb2.FindObservationReq(
                module_id = get_parameter(kwargs, "module_id"),
                obs_type = get_parameter(kwargs, "obs_type"),
                exp_time_start = get_parameter(kwargs, "obs_time", [None, None])[0],
                exp_time_end = get_parameter(kwargs, "obs_time", [None, None])[1],
                qc0_status = get_parameter(kwargs, "qc0_status"),
                prc_status = get_parameter(kwargs, "prc_status"),
                limit = get_parameter(kwargs, "limit", 0),
                other_conditions = {"test":"cnlab.test"}
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Observation, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            obs_id : [int] 

        return csst_dfs_common.models.Result
        '''
        try:
            obs_id = get_parameter(kwargs, "obs_id")
            resp, _ =  self.stub.Get.with_call(observation_pb2.GetObservationReq(
                obs_id = obs_id
            ),metadata = get_auth_headers())

            if resp.observation is None or resp.observation.id == 0:
                return Result.error(message=f"obs_id:{obs_id} not found")  

            return Result.ok_data(data=Observation.from_proto_model(resp.observation))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))        

    def update_proc_status(self, **kwargs):
        ''' update the status of reduction

        parameter kwargs:
            obs_id : [int],
            status : [int]

        return csst_dfs_common.models.Result
        '''
        obs_id = get_parameter(kwargs, "obs_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateProcStatus.with_call(
                observation_pb2.UpdateProcStatusReq(obs_id=obs_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def update_qc0_status(self, **kwargs):
        ''' update the status of QC0
        
        parameter kwargs:
            obs_id : [int],
            status : [int]
        '''        
        obs_id = get_parameter(kwargs, "obs_id")
        status = get_parameter(kwargs, "status")
        try:
            resp,_ = self.stub.UpdateQc0Status.with_call(
                observation_pb2.UpdateQc0StatusReq(obs_id=obs_id, status=status),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def write(self, **kwargs):
        ''' insert a observational record into database
 
        parameter kwargs:
            obs_time = [str]
            exp_time = [int]
            module_id = [str]
            obs_type = [str]
            facility_status_id = [int]
            module_status_id = [int]
        return: csst_dfs_common.models.Result
        '''   

        rec = observation_pb2.Observation(
            obs_time = get_parameter(kwargs, "obs_time"),
            exp_time = get_parameter(kwargs, "exp_time"),
            module_id = get_parameter(kwargs, "module_id"),
            obs_type = get_parameter(kwargs, "obs_type"),
            facility_status_id = get_parameter(kwargs, "facility_status_id"),
            module_status_id = get_parameter(kwargs, "module_status_id")
        )
        req = observation_pb2.WriteObservationReq(record = rec)
        try:
            resp,_ = self.stub.Write.with_call(req,metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Observation.from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

   

