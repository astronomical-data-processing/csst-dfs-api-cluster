import grpc
import pickle
from collections import deque
import logging
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
            datas = bytearray()
            totalCount = 0
            for resp in self.stub.Gaia3Search(ephem_pb2.EphemSearchRequest(
                ra = ra,
                dec = dec,
                radius = radius,
                minMag = min_mag,
                maxMag = max_mag,
                obstime = obstime,
                limit = limit
            ),metadata = get_auth_headers()):
                if resp.success:
                    # data = from_proto_model_list(Gaia3Record, resp.records)
                    data = resp.records
                    datas.extend(data)
                    totalCount = resp.totalCount
                else:
                    return Result.error(message = str(resp.error.detail))
            
            records = pickle.loads(datas)
            ret_records2 = []
            for r in records:
                rec = Gaia3Record(
                        SolutionId = r[0],
                        Designation = r[1],
                        SourceId = r[2],
                        RandomIndex = r[3],
                        RefEpoch = r[4],
                        Ra = r[5],
                        RaError = r[6],
                        Dec = r[7],
                        DecError = r[8],
                        Parallax = r[9],
                        ParallaxError = r[10],
                        ParallaxOverError = r[11],
                        Pm = r[12],
                        Pmra = r[13],
                        PmraError = r[14],
                        Pmdec = r[15],
                        PmdecError = r[16],
                        RaDecCorr = r[17],
                        RaParallaxCorr = r[18],
                        RaPmraCorr = r[19],
                        RaPmdecCorr = r[20],
                        DecParallaxCorr = r[21],
                        DecPmraCorr = r[22],
                        DecPmdecCorr = r[23],
                        ParallaxPmraCorr = r[24],
                        ParallaxPmdecCorr = r[25],
                        PmraPmdecCorr = r[26],
                        AstrometricNObsAl = r[27],
                        AstrometricNObsAc = r[28],
                        AstrometricNGoodObsAl = r[29],
                        AstrometricNBadObsAl = r[30],
                        AstrometricGofAl = r[31],
                        AstrometricChi2Al = r[32],
                        AstrometricExcessNoise = r[33],
                        AstrometricExcessNoiseSig = r[34],
                        AstrometricParamsSolved = r[35],
                        AstrometricPrimaryFlag = r[36],
                        NuEffUsedInAstrometry = r[37],
                        Pseudocolour = r[38],
                        PseudocolourError = r[39],
                        RaPseudocolourCorr = r[40],
                        DecPseudocolourCorr = r[41],
                        ParallaxPseudocolourCorr = r[42],   
                        PmraPseudocolourCorr = r[43],
                        PmdecPseudocolourCorr = r[44],
                        AstrometricMatchedTransits = r[45],
                        VisibilityPeriodsUsed = r[46],
                        AstrometricSigma5dMax = r[47],
                        MatchedTransits = r[48],
                        NewMatchedTransits = r[49],
                        MatchedTransitsRemoved = r[50],
                        IpdGofHarmonicAmplitude = r[51],
                        IpdGofHarmonicPhase = r[52],
                        IpdFracMultiPeak = r[53],
                        IpdFracOddWin = r[54],
                        Ruwe = r[55],
                        ScanDirectionStrengthK1 = r[56],
                        ScanDirectionStrengthK2 = r[57],
                        ScanDirectionStrengthK3 = r[58],
                        ScanDirectionStrengthK4 = r[59],
                        ScanDirectionMeanK1 = r[60],
                        ScanDirectionMeanK2 = r[61],
                        ScanDirectionMeanK3 = r[62],
                        ScanDirectionMeanK4 = r[63],
                        DuplicatedSource = r[64],
                        PhotGNObs = r[65],
                        PhotGMeanFlux = r[66],
                        PhotGMeanFluxError = r[67],
                        PhotGMeanFluxOverError = r[68],
                        PhotGMeanMag = r[69],
                        PhotBpNObs = r[70],
                        PhotBpMeanFlux = r[71],
                        PhotBpMeanFluxError = r[72],
                        PhotBpMeanFluxOverError = r[73],
                        PhotBpMeanMag = r[74],
                        PhotRpNObs = r[75],
                        PhotRpMeanFlux = r[76],
                        PhotRpMeanFluxError = r[77],
                        PhotRpMeanFluxOverError = r[78],
                        PhotRpMeanMag = r[79],
                        PhotBpRpExcessFactor = r[80],
                        PhotBpNContaminatedTransits = r[81],
                        PhotBpNBlendedTransits = r[82],
                        PhotRpNContaminatedTransits = r[83],
                        PhotRpNBlendedTransits = r[84],
                        PhotProcMode = r[85],
                        BpRp = r[86],
                        BpG = r[87],
                        GRp = r[88],
                        RadialVelocity = r[89],
                        RadialVelocityError = r[90],
                        RvMethodUsed = r[91],
                        RvNbTransits = r[92], 
                        RvNbDeblendedTransits = r[93],
                        RvVisibilityPeriodsUsed = r[94],
                        RvExpectedSigToNoise = r[95],
                        RvRenormalisedGof = r[96],
                        RvChisqPvalue = r[97],
                        RvTimeDuration = r[98],
                        RvAmplitudeRobust = r[99],
                        RvTemplateTeff = r[100],
                        RvTemplateLogg = r[101],
                        RvTemplateFeH = r[102],
                        RvAtmParamOrigin = r[103],
                        Vbroad = r[104],
                        VbroadError = r[105],
                        VbroadNbTransits = r[106],
                        GrvsMag = r[107],
                        GrvsMagError = r[108],
                        GrvsMagNbTransits = r[109],
                        RvsSpecSigToNoise = r[110],
                        PhotVariableFlag = r[111],
                        L = r[112],
                        B = r[113],
                        EclLon = r[114],
                        EclLat = r[115],
                        InQsoCandidates = r[116],
                        InGalaxyCandidates = r[117],
                        NonSingleStar = r[118],
                        HasXpContinuous = r[119],
                        HasXpSampled = r[120],
                        HasRvs = r[121],
                        HasEpochPhotometry = r[122],
                        HasEpochRv = r[123],
                        HasMcmcGspphot = r[124],
                        HasMcmcMsc = r[125],
                        InAndromedaSurvey = r[126],
                        ClassprobDscCombmodQuasar = r[127],
                        ClassprobDscCombmodGalaxy = r[128],
                        ClassprobDscCombmodStar = r[129],
                        TeffGspphot = r[130],
                        TeffGspphotLower = r[131],
                        TeffGspphotUpper = r[132],
                        LoggGspphot = r[133],
                        LoggGspphotLower = r[134],
                        LoggGspphotUpper = r[135],
                        MhGspphot = r[136],
                        MhGspphotLower = r[137],
                        MhGspphotUpper = r[138],
                        DistanceGspphot = r[139],
                        DistanceGspphotLower = r[140],
                        DistanceGspphotUpper = r[141],
                        AzeroGspphot = r[142],   
                        AzeroGspphotLower = r[143],
                        AzeroGspphotUpper = r[144],
                        AgGspphot = r[145],
                        AgGspphotLower = r[146],
                        AgGspphotUpper = r[147],
                        EbpminrpGspphot = r[148],
                        EbpminrpGspphotLower = r[149],
                        EbpminrpGspphotUpper = r[150],
                        LibnameGspphot = r[151],
                        NS8HIdx = r[152],
                        NS16HIdx = r[153],
                        NS32HIdx = r[154],
                        NS64HIdx = r[155],
                        FileIdx = r[156])
                ret_records2.append(rec)

            return Result.ok_data(data = ret_records2).append("totalCount", totalCount)
        except grpc.RpcError as e:
            return Result.error(message="%s:%s" % (e.code().value, e.details()))
