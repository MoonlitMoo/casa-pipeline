import os

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


def _calculate(worker, consider_flag=False):
    worker.SubtractMedian(threshold=3.0, consider_flag=consider_flag)
    worker.CalcStdSpectrum(consider_flag=consider_flag)
    worker.CalcRange(threshold=3.0, detection=5.0, extension=2.0, iteration=10, consider_flag=consider_flag)
    mask_list = worker.masklist
    return mask_list


class MaskDeviationHeuristic(api.Heuristic):
    def calculate(self, vis, field_id='', antenna_id='', spw_id='', consider_flag=False):
        """
        Channel mask heuristics using MaskDeviation algorithm implemented 
        in MaskDeviation class. 

        vis -- input MS filename
        field_id -- target field identifier
        antenna_id -- target antenna identifier
        spw -- target spw identifier
        consider_flag -- take into account flag in MS or not
        """
        worker = MaskDeviation(vis, spw_id)
        worker.ReadData(field=field_id, antenna=antenna_id)
        mask_list = _calculate(worker, consider_flag=consider_flag)
        del worker
        return mask_list


class MaskDeviation(object):
    """
    The class is used to detect channels having large variation or deviation. If there's any
    emission lines or atmospheric absorption/emission on some channels, their values largely
    change according to the positional and environmental changes. Emission lines and atmospheric
    features often degrade the quality of the baseline subtraction. Therefore, channels with
    large valiation should be masked before baseline fitting order determination and baseline
    subtraction.
    """
    def __init__(self, infile, spw=None):
        self.infile = infile.rstrip('/')
        self.spw = spw
        LOG.debug('MaskDeviation.__init__: infile %s spw %s'%(os.path.basename(self.infile), self.spw))
        self.masklist = []

    def ReadData(self, vis='', field='', antenna='', colname=None):
        """
        Reads data from input MS. 
        """
        if vis != '':
            self.infile=vis
        if vis == '':
            vis = self.infile
        spwsel = '' if self.spw is None else str(self.spw)
        mssel = {'field': str(field),
                 'spw': str(spwsel),
                 'scanintent': 'OBSERVE_TARGET#ON_SOURCE*'}
        LOG.debug('vis="%s"'%(vis))
        LOG.debug('mssel=%s'%(mssel))

        if colname is None:
            with casa_tools.TableReader(vis) as mytb:
                colnames = mytb.colnames()
            if 'CORRECTED_DATA' in colnames:
                colname = 'corrected_data'
            elif 'FLOAT_DATA' in colnames:
                colname = 'float_data'
            elif 'DATA' in colnames:
                colname = 'data'
            else:
                raise RuntimeError('{} doesn\'t have any data column (CORRECTED, FLOAT, DATA)'.format(os.path.basename(vis)))

        with casa_tools.MSReader(vis) as myms:
            mssel['baseline'] = '%s&&&'%(antenna)
            myms.msselect(mssel)
            r = myms.getdata([colname, 'flag'])
            npol, nchan, nrow = r['flag'].shape
            self.nrow = npol * nrow
            self.nchan = nchan
            self.data= np.real(r[colname.lower()]).transpose((2, 0, 1)).reshape((nrow * npol, nchan))
            self.flag = r['flag'].transpose((2, 0, 1)).reshape((nrow * npol, nchan))

        LOG.debug('MaskDeviation.ReadDataFromMS: %s %s'%(self.nrow, self.nchan))

        return r

    def SubtractMedian(self, threshold=3.0, consider_flag=False):
        """
        Subtract median value of the spectrum from the spectrum: re-bias the spectrum.

        Initial median (MED_0) and standard deviation (STD) are caluculated for each
        spectrum. Final median value is determined by using the channels having the value
        inside the range: MED_0 - threshold * STD < VALUE < MED_0 + threshold * STD
        """
        if hasattr(self, 'flag') and consider_flag:
            with_flag = True
        else:
            with_flag = False
        for i in range(self.nrow):
            if with_flag:
                if np.all(self.flag[i] == True):
                    continue
                median = np.median(self.data[i][self.flag[i] == False])
                std = self.data[i][self.flag[i] == False].std()
            else:
                median = np.median(self.data[i])
                std = self.data[i].std()
            # mask: True => valid, False => invalid
            mask = (self.data[i]<(median+threshold*std)) * (self.data[i]>(median-threshold*std))
            if with_flag:
                medianval = np.median(self.data[i][np.logical_and(mask == True, self.flag[i] == False)])
            else:
                medianval = np.median(self.data[i][np.where(mask == True)])
            LOG.trace('MaskDeviation.SubtractMedian: row %s %s %s %s %s %s'%(i, median, std, medianval, mask.sum(), self.nchan))
            self.data[i] -= medianval

    def CalcStdSpectrum(self, consider_flag=False):
        """
        meanSP, maxSP, minSP, ymax, ymin: used only for plotting and should be
         commented out when implemented in the pipeline
        """
        if hasattr(self, 'flag') and consider_flag:
            with_flag = True
        else:
            with_flag = False

        if with_flag:
            mdata = np.ma.masked_array(self.data, self.flag)
        else:
            mdata = self.data

        self.stdSP = mdata.std(axis=0)
        self.meanSP = mdata.mean(axis=0)
        self.maxSP = mdata.max(axis=0)
        self.minSP = mdata.min(axis=0)
        self.ymax = self.maxSP.max()
        self.ymin = self.minSP.min()

        LOG.trace('std %s\nmean %s\n max %s\n min %s\n ymax %s ymin %s' %
                  (self.stdSP, self.meanSP, self.maxSP, self.minSP, self.ymax, self.ymin))

    def CalcRange(self, threshold=3.0, detection=5.0, extension=2.0, iteration=10, consider_flag=False):
        """
        Find regions which value is greater than threshold.
        'threshold' is used for median calculation
        'detection' is used to detect mask region
        'extension' is used to extend the mask region
        Used data:
            self.stdSp: 1D spectrum with self.nchan channels calculated in CalcStdSpectrum
                        Each channel records standard deviation of the channel in all original spectra
        """
        if hasattr(self.stdSP, 'mask') and consider_flag:
            with_flag = True
            stdSP = self.stdSP.data
        else:
            with_flag = False
            stdSP = self.stdSP

        # mask: True => valid, False => invalid
        mask = (stdSP>-99999)

        if with_flag:
            mask = np.logical_and(mask, self.stdSP.mask == False)

        # if there is no valid data, do not evaluate deviation mask
        if np.all(np.logical_not(mask)):
            self.masklist = []
            return

        Nmask0 = 0
        for i in range(iteration):
            median = np.median(stdSP[np.where(mask == True)])
            std = stdSP[np.where(mask == True)].std()
            mask = stdSP<(median+threshold*std)
            #mask = (self.stdSP<(median+threshold*std)) * (self.stdSP>(median-threshold*std))
            if with_flag:
                mask = np.logical_and(mask, self.stdSP.mask == False)
            Nmask = mask.sum()
            LOG.trace('MaskDeviation.CalcRange: %s %s %s %s'%(median, std, Nmask, self.nchan))
            if Nmask == Nmask0: break
            else: Nmask0 = Nmask
        # TODO
        mask = stdSP<(median+detection*std)
        LOG.trace('MaskDeviation.CalcRange: before ExtendMask %s'%(mask))
        mask = self.ExtendMask(mask, median+extension*std)
        LOG.trace('MaskDeviation.CalcRange: after ExtendMask %s'%(mask))

        self.mask = np.arange(self.nchan)[np.where(mask == False)]
        LOG.trace('MaskDeviation.CalcRange: self.mask=%s'%(self.mask))
        RL = (mask*1)[1:]-(mask*1)[:-1]
        LOG.trace('MaskDeviation.CalcRange: RL=%s'%(RL))
        L = np.arange(self.nchan)[np.where(RL == -1)] + 1
        R = np.arange(self.nchan)[np.where(RL == 1)]
        if len(self.mask) > 0 and self.mask[0] == 0: L = np.insert(L, 0, 0)
        if len(self.mask) > 0 and self.mask[-1] == self.nchan-1: R = np.insert(R, len(R), self.nchan - 1)
        self.masklist = []
        for i in range(len(L)):
            self.masklist.append([L[i], R[i]])
        if len(self.mask) > 0:
            LOG.trace('MaskDeviation.CalcRange: %s %s %s %s %s'%(self.masklist, L, R, self.mask[0], self.mask[-1]))
        else:
            LOG.trace('MaskDeviation.CalcRange: %s %s %s'%(self.masklist, L, R))
        del mask, RL

    def ExtendMask(self, mask, threshold):
        """
        Extend the mask region as long as Standard Deviation value is higher than the given threshold
        """
        LOG.trace('MaskDeviation.ExtendMask: threshold = %s'%(threshold))
        for i in range(len(mask)-1):
            if (not mask[i]) and self.stdSP[i+1]>threshold: mask[i+1] = False
        for i in range(len(mask)-1, 1, -1):
            if (not mask[i]) and self.stdSP[i-1]>threshold: mask[i-1] = False
        return mask
