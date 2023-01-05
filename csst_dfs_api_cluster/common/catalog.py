import grpc
import pickle
from collections import deque
import logging
import io
import time

from csst_dfs_commons.models import Result
from csst_dfs_commons.models.common import from_proto_model_list, Gaia3Record

from csst_dfs_proto.common.ephem import ephem_pb2, ephem_pb2_grpc
from .service import ServiceProxy
from .constants import *
from .utils import get_auth_headers

log = logging.getLogger('csst')
class CatalogApi(object):
    def __init__(self):
        self.stub = ephem_pb2_grpc.EphemSearchSrvStub(ServiceProxy().channel())
    
    def gaia3_query(self, ra: float, dec: float, radius: float, columns: tuple, min_mag: float,  max_mag: float,  obstime: int, limit: int):
        ''' retrieval GAIA DR 3
            args:
                ra:  in deg
                dec:  in deg
                radius:  in deg
                tuple of str, like ('ra','dec','phot_g_mean_mag')
                min_mag: minimal magnitude
                max_mag: maximal magnitude
                obstime: seconds  
                limit: limits returns the number of records
            return: csst_dfs_common.models.Result
        ''' 
        try:
            datas = io.BytesIO()
            totalCount = 0
            t_start = time.time()
            resps = self.stub.Gaia3Search(ephem_pb2.EphemSearchRequest(
                ra = ra,
                dec = dec,
                radius = radius,
                columns = ",".join(columns),
                minMag = min_mag,
                maxMag = max_mag,
                obstime = obstime,
                limit = limit
            ),metadata = get_auth_headers())
            for resp in resps:
                if resp.success:
                    # data = from_proto_model_list(Gaia3Record, resp.records)
                    datas.write(resp.records)
                    totalCount = resp.totalCount
                else:
                    return Result.error(message = str(resp.error.detail))
            datas.flush()
            log.info("received used: %.6f's" %(time.time() - t_start,))
            t_start = time.time()
            records = pickle.loads(datas.getvalue())
            log.info("unserialization used: %.6f's" %(time.time() - t_start,))

            return Result.ok_data(data = records).append("totalCount", totalCount).append("columns", columns)
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
