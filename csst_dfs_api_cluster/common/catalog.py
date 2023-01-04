import grpc
import pickle
from collections import deque
import logging
import io

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
            return: csst_dfs_common.models.Result
        ''' 
        try:
            datas = io.BytesIO()
            totalCount = 0

            resps = self.stub.Gaia3Search(ephem_pb2.EphemSearchRequest(
                ra = ra,
                dec = dec,
                radius = radius,
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
            records = pickle.loads(datas.getvalue())
            ret_records2 = []
            for r in records:
                rec = Gaia3Record()
                rec.SolutionId = r[0]
                rec.Designation = r[1]
                rec.SourceId = r[2]
                rec.RandomIndex = r[3]
                rec.RefEpoch = r[4]
                rec.Ra = r[5]
                rec.RaError = r[6]
                rec.Dec = r[7]
                rec.DecError = r[8]
                rec.Parallax = r[9]
                rec.ParallaxError = r[10]
                rec.ParallaxOverError = r[11]
                rec.Pm = r[12]
                rec.Pmra = r[13]
                rec.PmraError = r[14]
                rec.Pmdec = r[15]
                rec.PmdecError = r[16]
                rec.RaDecCorr = r[17]
                rec.RaParallaxCorr = r[18]
                rec.RaPmraCorr = r[19]
                rec.RaPmdecCorr = r[20]
                rec.DecParallaxCorr = r[21]
                rec.DecPmraCorr = r[22]
                rec.DecPmdecCorr = r[23]
                rec.ParallaxPmraCorr = r[24]
                rec.ParallaxPmdecCorr = r[25]
                rec.PmraPmdecCorr = r[26]
                rec.AstrometricNObsAl = r[27]
                rec.AstrometricNObsAc = r[28]
                rec.AstrometricNGoodObsAl = r[29]
                rec.AstrometricNBadObsAl = r[30]
                rec.AstrometricGofAl = r[31]
                rec.AstrometricChi2Al = r[32]
                rec.AstrometricExcessNoise = r[33]
                rec.AstrometricExcessNoiseSig = r[34]
                rec.AstrometricParamsSolved = r[35]
                rec.AstrometricPrimaryFlag = r[36]
                rec.NuEffUsedInAstrometry = r[37]
                rec.Pseudocolour = r[38]
                rec.PseudocolourError = r[39]
                rec.RaPseudocolourCorr = r[40]
                rec.DecPseudocolourCorr = r[41]
                rec.ParallaxPseudocolourCorr = r[42]   
                rec.PmraPseudocolourCorr = r[43]
                rec.PmdecPseudocolourCorr = r[44]
                rec.AstrometricMatchedTransits = r[45]
                rec.VisibilityPeriodsUsed = r[46]
                rec.AstrometricSigma5dMax = r[47]
                rec.MatchedTransits = r[48]
                rec.NewMatchedTransits = r[49]
                rec.MatchedTransitsRemoved = r[50]
                rec.IpdGofHarmonicAmplitude = r[51]
                rec.IpdGofHarmonicPhase = r[52]
                rec.IpdFracMultiPeak = r[53]
                rec.IpdFracOddWin = r[54]
                rec.Ruwe = r[55]
                rec.ScanDirectionStrengthK1 = r[56]
                rec.ScanDirectionStrengthK2 = r[57]
                rec.ScanDirectionStrengthK3 = r[58]
                rec.ScanDirectionStrengthK4 = r[59]
                rec.ScanDirectionMeanK1 = r[60]
                rec.ScanDirectionMeanK2 = r[61]
                rec.ScanDirectionMeanK3 = r[62]
                rec.ScanDirectionMeanK4 = r[63]
                rec.DuplicatedSource = r[64]
                rec.PhotGNObs = r[65]
                rec.PhotGMeanFlux = r[66]
                rec.PhotGMeanFluxError = r[67]
                rec.PhotGMeanFluxOverError = r[68]
                rec.PhotGMeanMag = r[69]
                rec.PhotBpNObs = r[70]
                rec.PhotBpMeanFlux = r[71]
                rec.PhotBpMeanFluxError = r[72]
                rec.PhotBpMeanFluxOverError = r[73]
                rec.PhotBpMeanMag = r[74]
                rec.PhotRpNObs = r[75]
                rec.PhotRpMeanFlux = r[76]
                rec.PhotRpMeanFluxError = r[77]
                rec.PhotRpMeanFluxOverError = r[78]
                rec.PhotRpMeanMag = r[79]
                rec.PhotBpRpExcessFactor = r[80]
                rec.PhotBpNContaminatedTransits = r[81]
                rec.PhotBpNBlendedTransits = r[82]
                rec.PhotRpNContaminatedTransits = r[83]
                rec.PhotRpNBlendedTransits = r[84]
                rec.PhotProcMode = r[85]
                rec.BpRp = r[86]
                rec.BpG = r[87]
                rec.GRp = r[88]
                rec.RadialVelocity = r[89]
                rec.RadialVelocityError = r[90]
                rec.RvMethodUsed = r[91]
                rec.RvNbTransits = r[92] 
                rec.RvNbDeblendedTransits = r[93]
                rec.RvVisibilityPeriodsUsed = r[94]
                rec.RvExpectedSigToNoise = r[95]
                rec.RvRenormalisedGof = r[96]
                rec.RvChisqPvalue = r[97]
                rec.RvTimeDuration = r[98]
                rec.RvAmplitudeRobust = r[99]
                rec.RvTemplateTeff = r[100]
                rec.RvTemplateLogg = r[101]
                rec.RvTemplateFeH = r[102]
                rec.RvAtmParamOrigin = r[103]
                rec.Vbroad = r[104]
                rec.VbroadError = r[105]
                rec.VbroadNbTransits = r[106]
                rec.GrvsMag = r[107]
                rec.GrvsMagError = r[108]
                rec.GrvsMagNbTransits = r[109]
                rec.RvsSpecSigToNoise = r[110]
                rec.PhotVariableFlag = r[111]
                rec.L = r[112]
                rec.B = r[113]
                rec.EclLon = r[114]
                rec.EclLat = r[115]
                rec.InQsoCandidates = r[116]
                rec.InGalaxyCandidates = r[117]
                rec.NonSingleStar = r[118]
                rec.HasXpContinuous = r[119]
                rec.HasXpSampled = r[120]
                rec.HasRvs = r[121]
                rec.HasEpochPhotometry = r[122]
                rec.HasEpochRv = r[123]
                rec.HasMcmcGspphot = r[124]
                rec.HasMcmcMsc = r[125]
                rec.InAndromedaSurvey = r[126]
                rec.ClassprobDscCombmodQuasar = r[127]
                rec.ClassprobDscCombmodGalaxy = r[128]
                rec.ClassprobDscCombmodStar = r[129]
                rec.TeffGspphot = r[130]
                rec.TeffGspphotLower = r[131]
                rec.TeffGspphotUpper = r[132]
                rec.LoggGspphot = r[133]
                rec.LoggGspphotLower = r[134]
                rec.LoggGspphotUpper = r[135]
                rec.MhGspphot = r[136]
                rec.MhGspphotLower = r[137]
                rec.MhGspphotUpper = r[138]
                rec.DistanceGspphot = r[139]
                rec.DistanceGspphotLower = r[140]
                rec.DistanceGspphotUpper = r[141]
                rec.AzeroGspphot = r[142]   
                rec.AzeroGspphotLower = r[143]
                rec.AzeroGspphotUpper = r[144]
                rec.AgGspphot = r[145]
                rec.AgGspphotLower = r[146]
                rec.AgGspphotUpper = r[147]
                rec.EbpminrpGspphot = r[148]
                rec.EbpminrpGspphotLower = r[149]
                rec.EbpminrpGspphotUpper = r[150]
                rec.LibnameGspphot = r[151]
                rec.NS8HIdx = r[152]
                rec.NS16HIdx = r[153]
                rec.NS32HIdx = r[154]
                rec.NS64HIdx = r[155]
                rec.FileIdx = r[156]
                ret_records2.append(rec)
            return Result.ok_data(data = ret_records2).append("totalCount", totalCount)
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
