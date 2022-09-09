import grpc

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list
from csst_dfs_commons.models.facility import Level2Producer, Level2Job, Level2ProducerRuning

from csst_dfs_proto.facility.level2producer import level2producer_pb2, level2producer_pb2_grpc

from ..common.service import ServiceProxy
from ..common.utils import *
from ..common.constants import UPLOAD_CHUNK_SIZE

class Level2ProducerApi(object):
    """
    Level2Producer Operation Class
    """    
    def __init__(self):
        self.stub = level2producer_pb2_grpc.Level2ProducerSrvStub(ServiceProxy().channel())

    def register(self, **kwargs):
        ''' register a Level2Producer data record into database
 
        :param kwargs: Parameter dictionary, key items support:
            name = [str]\n
            gitlink = [str]\n
            paramfiles = [str]\n
            priority = [int]\n
            pre_producers = list[int]
        
        :returns: csst_dfs_common.models.Result
        '''  
        rec = level2producer_pb2.Level2ProducerRecord(
            id = get_parameter(kwargs, "id", 0),
            name = get_parameter(kwargs, "name", ""),
            gitlink = get_parameter(kwargs, "gitlink"),
            paramfiles = get_parameter(kwargs, "paramfiles"),
            priority = get_parameter(kwargs, "priority", 0),
            pre_producers = get_parameter(kwargs, "pre_producers",[]),
        )
        req = level2producer_pb2.RegisterReq(record = rec)
        try:
            resp,_ = self.stub.Register.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level2Producer().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find(self, **kwargs):
        ''' retrieve Level2Producer records from database

        :param kwargs: Parameter dictionary, key items support:
            key: [str]
            limit: limits returns the number of records,default 0:no-limit
        
        :returns: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.Find.with_call(level2producer_pb2.FindReq(
                key = get_parameter(kwargs, "key", "")
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Level2Producer, resp.records)).append("totalCount", resp.totalCount)
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
            p_id = get_parameter(kwargs, "id", 0)
            resp, _ =  self.stub.Get.with_call(level2producer_pb2.GetReq(
                id = p_id
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"{p_id} not found")  

            return Result.ok_data(data=Level2Producer().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))        

    def find_nexts(self, **kwargs):
        ''' retrieve Level2Producer records from database

        :param kwargs: Parameter dictionary, key items support:
            id : [int]
        
        :returns: csst_dfs_common.models.Result
        '''
        try:
            resp, _ =  self.stub.FindNexts.with_call(level2producer_pb2.FindNextsReq(
                id = get_parameter(kwargs, "id", 0)
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Level2Producer, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find_start(self, **kwargs):
        ''' retrieve Level2Producer records from database

        :param kwargs: Parameter dictionary, key items support:
            key : [str]
        
        :returns: csst_dfs_common.models.Result
        '''
        try:

            resp, _ =  self.stub.FindStart.with_call(level2producer_pb2.FindStartReq(
                key = get_parameter(kwargs, "key", "")
            ),metadata = get_auth_headers())

            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Level2Producer, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))            

    def update(self, **kwargs):
        ''' update a Level2Producer
        
        :param kwargs: Parameter dictionary, key items support:
            id : [int]\n
            name = [str]\n
            gitlink = [str]\n
            paramfiles = [str]\n
            priority = [int]\n
            pre_producers = list[int]
        
        :returns: csst_dfs_common.models.Result            
        '''   
        try:
            rec = level2producer_pb2.Level2ProducerRecord(
                    id = get_parameter(kwargs, "id", 0),
                    name = get_parameter(kwargs, "name", ""),
                    gitlink = get_parameter(kwargs, "gitlink", ""),
                    paramfiles = get_parameter(kwargs, "paramfiles", ""),
                    priority = get_parameter(kwargs, "priority", 0),
                    pre_producers = get_parameter(kwargs, "pre_producers",[])
            )            
            resp,_ = self.stub.Update.with_call(
                level2producer_pb2.UpdateReq(record = rec),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def delete(self, **kwargs):
        ''' delete a Level2Producer data
 
        :param kwargs: Parameter dictionary, key items support:
            id = [int]
        
        :returns: csst_dfs_common.models.Result
        '''     
        try:
            resp,_ = self.stub.Delete.with_call(
                level2producer_pb2.DeleteReq(
                    id = get_parameter(kwargs, "id", 0)),
                metadata = get_auth_headers()
            )
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def new_job(self, **kwargs):
        ''' new a Level2Producer Job
 
        :param kwargs: Parameter dictionary, key items support:
            dag = [str]
        
        :returns: csst_dfs_common.models.Result
        '''  
        rec = level2producer_pb2.Level2JobRecord(
            id = 0,
            name = get_parameter(kwargs, "name", ""),
            dag = get_parameter(kwargs, "dag", "")
        )
        req = level2producer_pb2.NewJobReq(record = rec)
        try:
            resp,_ = self.stub.NewJob.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level2Job().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get_job(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int]

        return csst_dfs_common.models.Result
        '''
        try:
            p_id = get_parameter(kwargs, "id", 0)
            resp, _ =  self.stub.GetJob.with_call(level2producer_pb2.GetJobReq(
                id = p_id
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"{p_id} not found")  

            return Result.ok_data(data=Level2Job().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))  

    def update_job(self, **kwargs):
        ''' update a Level2Producer Job
 
        :param kwargs: Parameter dictionary, key items support:
            id = [int]
            dag = [str]
            status = [int]
        
        :returns: csst_dfs_common.models.Result
        '''    
        rec = level2producer_pb2.Level2JobRecord(
            id = get_parameter(kwargs, "id", 0), 
            name = get_parameter(kwargs, "name", ""),
            dag = get_parameter(kwargs, "dag", ""),
            status = get_parameter(kwargs, "status", -1)
        )
        req = level2producer_pb2.UpdateJobReq(record = rec)
        try:
            resp,_ = self.stub.UpdateJob.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))   

    def new_running(self, **kwargs):
        ''' insert a Level2ProducerRuningRecord data
 
        :param kwargs: Parameter dictionary, key items support:
            job_id = [int]\n
            producer_id = [int]\n
            brick_id = [int]\n
            start_time = [str]\n
            end_time = [str]\n
            prc_status = [int]\n
            prc_result = [str]
        
        :returns: csst_dfs_common.models.Result
        ''' 
        rec = level2producer_pb2.Level2ProducerRuningRecord(
            id = 0,
            job_id = get_parameter(kwargs, "job_id", 0),
            producer_id = get_parameter(kwargs, "producer_id", 0),
            brick_id = get_parameter(kwargs, "brick_id", 0),
            start_time = get_parameter(kwargs, "start_time", ""),
            prc_status = get_parameter(kwargs, "prc_status", 0),
            prc_result = get_parameter(kwargs, "prc_result", "")
        )
        req = level2producer_pb2.WriteRunningReq(record = rec)
        try:
            resp,_ = self.stub.WriteRunning.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data=Level2ProducerRuning().from_proto_model(resp.record))
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def get_running(self, **kwargs):
        '''  fetch a record from database

        parameter kwargs:
            id : [int]

        return csst_dfs_common.models.Result
        '''
        try:
            p_id = get_parameter(kwargs, "id", 0)
            resp, _ =  self.stub.GetRunning.with_call(level2producer_pb2.GetRunningReq(
                id = p_id
            ),metadata = get_auth_headers())

            if resp.record is None or resp.record.id == 0:
                return Result.error(message=f"{p_id} not found")  

            return Result.ok_data(data=Level2ProducerRuning().from_proto_model(resp.record))
           
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))  

    def update_running(self, **kwargs):
        ''' udpate a Level2ProducerRuningRecord data
 
        :param kwargs: Parameter dictionary, key items support:
            id = [int]\n
            job_id = [int]\n
            producer_id = [int]\n
            brick_id = [int]\n
            start_time = [str]\n
            end_time = [str]\n
            prc_status = [int]\n
            prc_result = [str]
        
        :returns: csst_dfs_common.models.Result
        ''' 
        rec = level2producer_pb2.Level2ProducerRuningRecord(
            id = get_parameter(kwargs, "id", 0),
            job_id = get_parameter(kwargs, "job_id", 0),
            producer_id = get_parameter(kwargs, "producer_id", 0),
            brick_id = get_parameter(kwargs, "brick_id", 0),
            start_time = get_parameter(kwargs, "start_time", ""),
            end_time = get_parameter(kwargs, "end_time", ""),
            prc_status = get_parameter(kwargs, "prc_status", 0),
            prc_result = get_parameter(kwargs, "prc_result", "")
        )
        req = level2producer_pb2.UpdateRunningReq(record = rec)
        try:
            resp,_ = self.stub.UpdateRunning.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data()
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

    def find_running(self, **kwargs):
        ''' find Level2ProducerRuningRecord data
 
        :param kwargs: Parameter dictionary, key items support:
            job_id = [int]\n
            producer_id = [int]\n
            brick_id = [int]\n
            prc_status = [int]\n
            create_time : (start, end)\n
            limit = [int]
        
        :returns: csst_dfs_common.models.Result
        '''  
        req = level2producer_pb2.FindRunningReq(
            job_id = get_parameter(kwargs, "job_id", 0),
            producer_id = get_parameter(kwargs, "producer_id", 0),
            brick_id = get_parameter(kwargs, "brick_id", 0),
            prc_status = get_parameter(kwargs, "prc_status", 0),
            start_time = get_parameter(kwargs, "create_time", [None, None])[0],
            end_time = get_parameter(kwargs, "create_time", [None, None])[1],
            limit = get_parameter(kwargs, "limit", 0)           
        )
        try:
            resp,_ = self.stub.FindRunning.with_call(req, metadata = get_auth_headers())
            if resp.success:
                return Result.ok_data(data = from_proto_model_list(Level2ProducerRuning, resp.records)).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = str(resp.error.detail))
    
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))            