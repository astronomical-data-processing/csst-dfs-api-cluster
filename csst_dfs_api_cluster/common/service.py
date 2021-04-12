import os
import grpc
class ServiceProxy:
    def __init__(self):
        self.gateway = os.getenv("CSST_DFS_GATEWAY",'172.31.248.218:30880')

    def insecure(self, stubCls):
        options = [('grpc.max_send_message_length', 1000 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                    ('grpc.enable_retries', 1),
                    ('grpc.service_config', '{ "retryPolicy":{ "maxAttempts": 4, "initialBackoff": "0.1s", "maxBackoff": "1s", "backoffMutiplier": 2, "retryableStatusCodes": [ "UNAVAILABLE" ] } }')]        
        channel = grpc.insecure_channel(self.gateway, options=options)
        return stubCls(channel)