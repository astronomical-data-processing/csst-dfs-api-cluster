import grpc
from csst_dfs_commons.models import Result
from csst_dfs_proto.common.ephem import ephem_pb2, ephem_pb2_grpc
from ..common.service import ServiceProxy
from ..common.constants import *

class CatalogApi(object):
    def __init__(self):
        self.proxy = ServiceProxy()
        self.stub = self.proxy.insecure(ephem_pb2_grpc.EphemSearchSrvStub)
    
    def gaia3_query(self, ra: float, dec: float, radius: float, min_mag: float,  max_mag: float,  obstime: int, limit: int):
        ''' retrieval GAIA DR 3
            args:
                ra:  in deg
                dec:  in deg
                radius:  in deg
                min_mag: minimal magnitude
                max_mag: maximal magnitude
                obstime: seconds  
                limit: limits returns the number of records
            return: a dict as {success: true, totalCount: 100, records:[.....]}
        ''' 
        try:
            resp = self.stub.Gaia3Search(ephem_pb2.EphemSearchRequest(
                ra = ra,
                dec = dec,
                radius = radius,
                minMag = min_mag,
                maxMag = max_mag,
                obstime = obstime,
                limit = limit
            ))

            if resp.success:
                return Result.ok_data(data=resp.records).append("totalCount", resp.totalCount)
            else:
                return Result.error(message = resp.message)

        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details))

