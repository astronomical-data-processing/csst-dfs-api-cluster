import os
import grpc
from csst_dfs_commons.models.errors import CSSTFatalException
class ServiceProxy:
    def __init__(self):
        self.gateway = os.getenv("CSST_DFS_GATEWAY",'172.31.248.218:30880')

    def channel(self):
        options = [('grpc.max_send_message_length', 1000 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                    ('grpc.enable_retries', 1),
                    ('grpc.service_config', '{ "retryPolicy":{ "maxAttempts": 4, "initialBackoff": "0.1s", "maxBackoff": "1s", "backoffMutiplier": 2, "retryableStatusCodes": [ "UNAVAILABLE" ] } }')]        
        channel = grpc.insecure_channel(self.gateway, options=options)
        try:
            grpc.channel_ready_future(channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            raise CSSTFatalException('Error connecting to server {}'.format(self.gateway))
        else:
            return channel