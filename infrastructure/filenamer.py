import os
import shutil
import string
import re

from . import utils

_valid_chars = "_.-+%s%s" % (string.ascii_letters, string.digits)

_known_intents = {
    'BANDPASS': 'bp',
    'AMPLITUDE': 'flux',
    'PHASE': 'ph',
    'TARGET': 'sci',
    'CHECK': 'chk',
    'POLARIZATION': 'pol',
    'POLANGLE': 'pang',
    'POLLEAKAGE': 'plk'
}


def _char_replacer(s, valid_chars):
    """A small utility function that echoes the argument or returns '_' if the
    argument is in a list of forbidden characters.
    """
    if s not in valid_chars:
        return '_'
    return s


# Allow _target.ms, _targets.ms or .ms endings: needed to import
# Measurement Sets (see PIPE-579, PIPE-1082, PIPE-1112, PIPE-1544)
def sanitize_for_ms(vis_name: str) -> str:
    """Remove suffix from MS name.

    Sanitized name is generated from input MS name by removing
    suffix. This method recognizes ".ms", "_target.ms", and
    "_targets.ms" as suffix.

    Args:
        vis_name: Name of MS

    Returns:
        Sanitized name
    """
    for msend in ('_target.ms', '_targets.ms', '.ms'):
        if vis_name.endswith(msend):
            return sanitize_for_ms(vis_name[:-len(msend)])
    return vis_name


def sanitize(text, valid_chars=None):
    if valid_chars is None:
        valid_chars = _valid_chars
    filename = ''.join(_char_replacer(c, valid_chars) for c in text)
    return filename


def fitsname(products_dir, imagename, version=1):

    """Strip off stage and iter information to generate
       FITS file name."""

    # Need to remove stage / iter information
    # fitsname = re.sub(r'\.s\d+.*\.iter.*\.', '.', imagename)
    fitsname = re.sub(r'\.s\d+[_]\d+\.', '.', imagename)
    fitsname = re.sub(r'\.iter\d+\.image', '', fitsname)
    fitsname = re.sub(r'\.iter\d+\.image.sd', '.sd', fitsname)
    fitsname = re.sub(r'\.iter\d+\.image.pbcor', '.pbcor', fitsname)
    fitsname = re.sub(r'\.iter\d+\.mask', '.mask', fitsname)
    fitsname = re.sub(r'\.iter\d+\.alpha', '.alpha', fitsname)
    # .pb must be tried after .pbcor.image !
    fitsname = re.sub(r'\.iter\d+\.pb', '.pb', fitsname)
    fitsfile = os.path.join(products_dir,
                            os.path.basename(fitsname) + '.fits')

    # update fitsname
    if version > 1:
        (aa, bb) = os.path.splitext(fitsfile)
        fitsfile = ''.join((aa, '.v', str(version), bb))

    return fitsfile


class FileNameComponentBuilder(object):
    def __init__(self):
        self._asdm = None
        self._task = None
        self._extension = None
        self._format = None
        self._intent = None
        self._iteration = None
        self._line_region = None
        self._datatype = None
        self._output_dir = None
        self._polarization = None
        self._antenna = None
        self._source = None
        self._spectral_window = None
        self._spectral_window_nochan = None
        self._band = None
        self._specmode = None
        self._type = None

        # these associations are not in the file naming proposal, but are used
        # for temporary files and output
        self._flag_marks = None
        self._method = None
        self._solint = None
        self._smooth = None
        self._stage = None

    def build(self):
        # The file names will be assembled using the order of the attributes
        # given here
        attributes = (os.path.basename(self._asdm),
                      self._task,
                      self._stage,
                      '_'.join([x for x in (self._source, self._intent) if x is not None]),
                      self._spectral_window,
                      self._spectral_window_nochan,
                      self._band,
                      self._specmode,
                      self._line_region,
                      self._datatype,
                      self._polarization,
                      self._antenna,
                      self._type,
                      self._method,
                      self._solint,
                      self._smooth,
                      self._extension,
                      self._iteration,
                      self._flag_marks,
                      self._format)
        basename = '.'.join([sanitize(x) for x in attributes
                             if x not in ('', None)])

        if self._output_dir is not None:
            return os.path.join(self._output_dir, basename)
        else:
            return basename

    def asdm(self, uid):
        self._asdm = uid
        return self

    def task(self, task):
        self._task = task
        return self

    def output_dir(self, output_dir):
        self._output_dir = output_dir
        return self

    def extension(self, extension):
        self._extension = extension
        return self

    def flag_marks(self, flag_marks):
        if flag_marks is None or flag_marks == '':
            self._flag_marks = None
        else:
            self._flag_marks = 'fm' + str(flag_marks)
        return self

    def format(self, format):
        self._format = format
        return self

    def intent(self, intent):
        self._intent = intent
        return self

    def iteration(self, iteration):
        self._iteration = 'iter' + str(iteration)
        return self

    def line_region(self, start_channel, end_channel):
        self._line_region = '_'.join((str(start_channel), str(end_channel)))
        return self

    def method(self, method):
        if method not in [None, 'None', '']:
            v = str(method).replace('.', '_')
            self._method = v
        else:
            self._method = None
        return self

    def datatype(self, datatype):
        self._datatype = datatype
        return self

    def polarization(self, polarization):
        self._polarization = polarization
        return self

    def antenna(self, antenna):
        self._antenna = antenna
        return self

    def solint(self, solint):
        if solint not in [None, 'None', '']:
            v = str(solint).replace('.', '_')
            self._solint = 'solint' + v
        else:
            self._solint = None
        return self

    def smooth(self, smooth):
        if smooth not in [None, 'None', '']:
            v = str(smooth).replace('.', '_')
            self._smooth = 'sm' + v
        else:
            self._smooth = None
        return self

    def source(self, source_name):
        self._source = source_name
        return self

    def spectral_window_nochan(self, window, virtspw=False):
        if window not in [None, 'None', '']:
            spw_inlist = window.split(',')
            spw_outlist = []
            for spw in spw_inlist:
                item = spw.split(':')[0]
                spw_outlist.append(item)
            if virtspw:
                self._spectral_window = 'virtspw' + sort_spws(','.join(spw_outlist))
            else:
                self._spectral_window = 'spw' + sort_spws(','.join(spw_outlist))
        else:
            self._spectral_window = None
        return self

    def spectral_window(self, window, virtspw=False):
        if window not in [None, 'None', '']:
            if virtspw:
                self._spectral_window = 'virtspw' + sort_spws(str(window))
            else:
                self._spectral_window = 'spw' + sort_spws(str(window))
        else:
            self._spectral_window = None
        return self

    def band(self, band):
        if band not in (None, 'None', ''):
            self._band = str(band)
        else:
            self._band = None
        return self

    def specmode(self, specmode):
        if specmode not in [None, 'None', '']:
            self._specmode = str(specmode)
        else:
            self._specmode = None
        return self

    def stage(self, stage):
        if stage not in [None, 'None', '']:
            self._stage = 's' + str(stage)
        else:
            self._stage = None
        return self

    def type(self, type):
        self._type = type
        return self


def sort_spws(unsorted):
    if not isinstance(unsorted, str) or ',' not in unsorted:
        return unsorted
    vals = unsorted.split(',')
    return ','.join(utils.numeric_sort(vals))


class NamingTemplate(object):
    """Base class used for all naming templates."""
    task = None

    def __init__(self):
        self._associations = FileNameComponentBuilder()

    def get_filename(self):
        """Assembles and returns the final filename."""
        filename_components = self._associations.build()
        filename = '.'.join([filename_components])
        return filename

    def output_dir(self, output_dir):
        """Set the base output directory for this template."""
        self._associations.output_dir(output_dir)
        return self

    def __repr__(self):
        return self.get_filename()


class ASDM(NamingTemplate):
    """Defines the ASDM naming scheme.

    ASDM file names have the syntax <project code>.<ASDM UID>.asdm,
    eg. pcode.uid___X02_X3d737_X1.asdm.
    """
    def __init__(self, other=None):
        """Creates a new ASDM naming template.

        If another naming template is given as a constructor argument,
        the new ASDM template will be initialized using applicable
        filename components copied from the given constructor argument.
        """
        super(ASDM, self).__init__()
        self._associations.format('asdm')
        if other is not None:
            self.asdm(other._associations._asdm)

    def asdm(self, uid):
        """Set the ASDM UID for this template, eg. uid://X03/XA83C/X02."""
        self._associations.asdm(uid)
        return self


class MeasurementSet(NamingTemplate):
    """Defines the measurement set naming scheme.

    File names have the syntax:
    <project code>.<ASDM UID>.ms.tbl, eg. pcode.uid___X02_X3d737_X1.ms.tbl.
    """
    def __init__(self, other=None):
        """
        Creates a new measurement set naming template.

        If another naming template is given as a constructor argument,
        the new measurement set template will be initialized using
        applicable filename components copied from the given
        constructor argument.
        """
        super(MeasurementSet, self).__init__()
        self._associations.extension('ms')
        self._associations.format('tbl')
        if other is not None:
            self.asdm(other._associations._asdm)

    def asdm(self, uid):
        """Set the ASDM UID for this template, eg. uid://X03/XA83C/X02."""
        self._associations.asdm(uid)
        return self


class FlaggingTable(NamingTemplate):
    """Defines the flagging table naming scheme.

    File names have the syntax:
    <ASDM UID>.flags.tbl, eg. uid___X02_X3d737_X1.flags.tbl.
    """
    def __init__(self, other=None):
        """Creates a new measurement set naming template.

        If another naming template is given as a constructor argument,
        the new measurement set template will be initialized using
        applicable filename components copied from the given
        constructor argument.
        """
        super(FlaggingTable, self).__init__()
        self._associations.task(self.task)
        self._associations.extension('flags')
        self._associations.format('tbl')
        if other is not None:
            self.asdm(other._associations._asdm)

    def asdm(self, uid):
        """Set the ASDM UID for this template, eg. uid://X03/XA83C/X02."""
        self._associations.asdm(uid)
        return self


class CalibrationTable(NamingTemplate):
    """
    Defines the calibration table naming scheme.

    File names have the syntax:
    <ASDM UID>.<spgrp>.<pol>.<fit>.<type>.<format>,
    eg. uid___X02_X3d737_X1.spwgrp1.X.channel.bcal.tbl.
    """
    _extensions = ['bcal', 'dcal', 'fcal', 'gcal', 'gacal', 'gpcal', 'pcal']
    _type = ['channel', 'poly', 'spline', 'tseries']

    def __init__(self, other=None):
        super(CalibrationTable, self).__init__()
        self._associations.task(CalibrationTable.task)
        self._associations.format('tbl')

        # if we've been given another namer to base ourselves on, we copy only
        # those parameters that are relevant to calibration tables.
        if other is not None:
            self.asdm(other._associations._asdm)
            self._associations._spectral_window = other._associations._spectral_window
            if other._associations._type in self._type:
                self.type(other._associations._type)
            self.polarization(other._associations._polarization)
            if other._associations._extension in self._extensions:
                self.extension(other._associations._extension)

        # should we also copy flag_marks, method etc.?

    def asdm(self, uid):
        """Set the ALMA project code for this template, eg. 2010.03.S."""
        self._associations.asdm(uid)
        return self

    def extension(self, extension):
        """Set the extension for this calibration table.

        The extension is not validated against the known set of
        calibration table extensions, so where possible it is
        preferable that you use one of the calibration type convenience
        methods: bandpass_cal(), focus_cal(), delay_cal() etc.
        """
        self._associations.extension(extension)
        return self

    def flag_marks(self, flag_marks):
        """Set the flag marks tag for this calibration table.

        The flag marks tag is a free parameter, and is not currently
        part of the file naming scheme proposal.
        """
        self._associations.flag_marks(flag_marks)
        return self

    def method(self, method):
        """Set the calibration method for this calibration table.

        The method argument is a free parameter, and is not currently
        part of the file naming scheme proposal. It is used to show how
        a particular calibration was calculated - in addition to the
        'type' parameter that should also be set on a calibration table.
        """
        self._associations.method(method)
        return self

    def polarization(self, polarization):
        """Set the polarization for this calibration table,
        eg. 'XX', 'YY'.
        """
        self._associations.polarization(polarization)
        return self

    def solint(self, solint):
        """Set the solution interval tag for this calibration table.

        The solution interval tag is a free parameter, and is not
        currently part of the file naming scheme proposal.
        """
        self._associations.solint(solint)
        return self

    def smooth(self, smooth):
        """Set the smoothing width for this calibration table.

        The smoothing width is a free parameter, and is not currently
        part of the file naming scheme proposal.
        """
        self._associations.smooth(smooth)
        return self

    def source(self, source):
        self._associations.source(source)
        return self

    def spectral_window(self, window):
        self._associations.spectral_window(window)
        return self

    def spectral_window_nochan(self, window):
        self._associations.spectral_window_nochan(window)
        return self

    def stage(self, stage):
        self._associations.stage(stage)
        return self

    def type(self, type):
        """Set the type component for this calibration table.

        The type is not validated, so where possible it is preferable
        that you use one of the fit type convenience methods, eg.
        channel_fit(), poly_fit(), spline_fit() etc.
        """
        self._associations.type(type)
        return self

    # Calibration type convenience methods ------------------------------------

    def amplitude_cal(self):
        """Set the filename extension as appropriate for an amplitude
        calibration.
        """
        return self.extension('ampcal')

    def bandpass_cal(self):
        """Set the filename extension as appropriate for a bandpass
        calibration.
        """
        return self.extension('bcal')

    def delay_cal(self):
        """Set the filename extension as appropriate for a delay
        calibration.
        """
        return self.extension('dcal')

    def flux_cal(self):
        """Set the filename extension as appropriate for a flux
        calibration.
        """
        return self.extension('fcal')

    def gain_cal(self):
        """Set the filename extension as appropriate for a gain
        calibration.
        """
        return self.extension('gcal')

    def phase_only_gain_cal(self):
        """Set the filename extension as appropriate for a phase-only
        gain calibration.
        """
        return self.extension('gpcal')

    def amplitude_only_gain_cal(self):
        """Set the filename extension as appropriate for an
        amplitude-only gain calibration.
        """
        return self.extension('gacal')

    def polarization_cal(self):
        """Set the filename extension as appropriate for a polarization
        calibration.
        """
        return self.extension('pcal')

    def antpos_cal(self):
        """Set the filename extension as appropriate for an antpos
        calibration.
        """
        return self.extension('ants')

    def uvcont_cal(self):
        """Set the filename extension as appropriate for a uv continuum
        calibration.
        """
        return self.extension('uvcont')

    def tsys_cal(self):
        """Set the filename extension as appropriate for a tsys
        calibration.
        """
        return self.extension('tsyscal')

    def opac_cal(self):
        """Set the filename extension as appropriate for a opac
        calibration.
        """
        return self.extension('opac')

    def gc_cal(self):
        """Set the filename extension as appropriate for a gc
        calibration.
        """
        return self.extension('gc')

    def rq_cal(self):
        """Set the filename extension as appropriate for a rq
        calibration.
        """
        return self.extension('rq')

    def swpow_cal(self):
        """Set the filename extension as appropriate for a swpow
        calibration.
        """
        return self.extension('swpow')

    def tecim_cal(self):
        """Set the filename extension as appropriate for a tecmaps
        calibration.
        """
        return self.extension('tecim')

    def wvrg_cal(self):
        """Set the filename extension as appropriate for a wvr
        calibration.
        """
        return self.extension('wvrcal')

    def xyf0_cal(self):
        """Set the filename extension as appropriate for a wvr
        calibration.
        """
        return self.extension('xyf0cal')

    def sdsky_cal(self):
        """Set the filename extension as appropriate for a single
        dish sky calibration."""
        return self.extension('skycal')

    def sdbaseline(self):
        """Set the filename extension as appropriate for a single
        dish baseline subtraction."""
        return self.extension('bl')

    # Fit type convenience methods --------------------------------------------

    def channel_fit(self):
        """Set the 'type' filename component for a per-channel
        calibration fit.
        """
        return self.type('channel')

    def poly_fit(self):
        """Set the 'type' filename component for a polynomial
        calibration fit.
        """
        return self.type('poly')

    def spline_fit(self):
        """Set the 'type' filename component for a spline calibration
        fit.
        """
        return self.type('spline')

    def time_series_fit(self):
        """Set the 'type' filename component for a time-series
        calibration fit.
        """
        return self.type('tseries')


class CASALog(NamingTemplate):
    def __init__(self, other=None):
        super(CASALog, self).__init__()
        self._associations.extension('log')
        self._associations.format('txt')
        self._associations.type('casapy')


class Image(NamingTemplate):
    def __init__(self, virtspw=False):
        # If virtspw is True, the filename will contain "virtspw" instead of "spw".
        # It was considered for PL2021, but deferred to later. In that case the
        # above default should be changed to True.
        self.virtspw = virtspw
        super(Image, self).__init__()

    def flag_marks(self, flag_marks):
        """Set the flag marks tag for this image.

        The flag marks tag is a free parameter, and is not currently
        part of the file naming scheme proposal.
        """
        self._associations.flag_marks(flag_marks)
        return self

    def intent(self, intent):
        if intent in _known_intents:
            intent = _known_intents[intent]
        self._associations.intent(intent)
        return self

    def iteration(self, iteration):
        self._associations.iteration(iteration)
        return self

    def line_region(self, start_channel, end_channel):
        self._associations.line_region(start_channel, end_channel)
        return self

    def datatype(self, datatype):
        self._associations.datatype(datatype)
        return self

    def polarization(self, polarization):
        self._associations.polarization(polarization)
        return self

    def source(self, source_name):
        self._associations.source(source_name)
        return self

    def stage(self, stage):
        self._associations.stage(stage)
        return self

    def spectral_window(self, window):
        self._associations.spectral_window(window, virtspw=self.virtspw)
        return self

    def specmode(self, specmode):
        self._associations.specmode(specmode)
        return self

    def type(self, type):
        self._associations.type(type)
        return self

    def band(self, band):
        self._associations.band(band)
        return self

    def antenna(self, antenna):
        self._associations.antenna(antenna)
        return self

    # Association convenience methods ----------------------------------------

    def continuum_image(self):
        self._associations.format('cim')
        return self

    def spectral_image(self):
        self._associations.format('spim')
        return self

    def clean(self):
        self._associations.extension('clean')
        return self

    def clean_mask(self):
        self._associations.extension('mask')
        return self

    def dirty(self):
        self._associations.extension('dirty')
        return self

    def model(self):
        self._associations.extension('model')
        return self

    def residual(self):
        self._associations.extension('residual')
        return self

    def psf(self):
        self._associations.extension('psf')
        return self

    def integrated_fluxscale(self):
        self._associations.extension('ifxscl')
        return self

    def fluxscale(self):
        self._associations.extension('fxscl')
        return self

    def flat_flux_clean(self):
        self._associations.extension('ffcln')
        return self

    def flat_flux_residual(self):
        self._associations.extension('ffres')
        return self

    def single_dish(self):
        self._associations.extension('sd')

    # Intent convenience methods --------------------------------------------

    def bandpass(self):
        return self.intent('bp')

    def flux(self):
        return self.intent('flux')

    def gain(self):
        return self.intent('gain')

    def science(self):
        return self.intent('sci')


class MosaicImage(Image):

    def flag_marks(self, flag_marks):
        """Set the flag marks tag for this image.

        The flag marks tag is a free parameter, and is not currently
        part of the file naming scheme proposal.
        """
#        self._associations.flag_marks(flag_marks)
        return self


class AmplitudeCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(AmplitudeCalibrationTable, self).__init__(other)
        self.amplitude_cal()


class AntposCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(AntposCalibrationTable, self).__init__(other)
        self.antpos_cal()


class UVcontCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(UVcontCalibrationTable, self).__init__(other)
        self.uvcont_cal()


class BandpassCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(BandpassCalibrationTable, self).__init__(other)
        self.bandpass_cal()


class DelayCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(DelayCalibrationTable, self).__init__(other)
        self.delay_cal()


class PolCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super().__init__(other)
        self.polarization_cal()


class FluxCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(FluxCalibrationTable, self).__init__(other)
        self.flux_cal()


class GainCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(GainCalibrationTable, self).__init__(other)
        self.gain_cal()


class XYf0CalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(XYf0CalibrationTable, self).__init__(other)
        self.xyf0_cal()


class TsysCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(TsysCalibrationTable, self).__init__(other)
        self.tsys_cal()


class OpCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(OpCalibrationTable, self).__init__(other)
        self.opac_cal()


class GainCurvesCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(GainCurvesCalibrationTable, self).__init__(other)
        self.gc_cal()


class RqCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(RqCalibrationTable, self).__init__(other)
        self.rq_cal()


class TecMapsCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(TecMapsCalibrationTable, self).__init__(other)
        self.tecim_cal()


class SwpowCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(SwpowCalibrationTable, self).__init__(other)
        self.swpow_cal()


class WvrgCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(WvrgCalibrationTable, self).__init__(other)
        self.wvrg_cal()


class SDSkyCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(SDSkyCalibrationTable, self).__init__(other)
        self.sdsky_cal()


class SDBaselineTable(CalibrationTable):
    def __init__(self, other=None):
        super(SDBaselineTable, self).__init__(other)
        self.sdbaseline()


# product name utility
class PipelineProductNameBuilder(object):

    @classmethod
    def __build(cls, *args, **kwargs):
        if 'separator' in kwargs:
            separator = kwargs['separator']
        else:
            separator = '.'
        return separator.join(map(str, args))

    @classmethod
    def _join_dir(cls, name, output_dir=None):
        if output_dir is not None:
            name = os.path.join(output_dir, name)
        return name

    @classmethod
    def _build_from_oussid(cls, basename, ousstatus_entity_id=None, output_dir=None):
        if ousstatus_entity_id is None:
            name = basename
        else:
            name = cls.__build(ousstatus_entity_id, basename)
        return cls._join_dir(name, output_dir)

    @classmethod
    def _build_from_ps_oussid(cls, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        if project_structure is None:
            name = basename
        elif project_structure.ousstatus_entity_id == 'unknown':
            name = basename
        else:
            name = cls._build_from_oussid(basename, ousstatus_entity_id=ousstatus_entity_id)
        return cls._join_dir(name, output_dir)

    @classmethod
    def _build_from_oussid_session(cls, basename, ousstatus_entity_id=None, session_name=None, output_dir=None):
        name = cls.__build(ousstatus_entity_id, session_name, basename)
        return cls._join_dir(name, output_dir)

    @classmethod
    def _build_calproduct_name(cls, basename, aux_product=False, output_dir=None):
        if aux_product:
            prefix = 'auxcal'
        else:
            prefix = 'cal'
        name = cls.__build(prefix, basename, separator='')
        return cls._join_dir(name, output_dir)

    @classmethod
    def _build_from_vis(cls, basename, vis, output_dir=None):
        name = cls.__build(os.path.basename(vis), basename)
        return cls._join_dir(name, output_dir)

    @classmethod
    def weblog(cls, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return cls._build_from_ps_oussid('weblog.tgz',
                                         project_structure=project_structure,
                                         ousstatus_entity_id=ousstatus_entity_id,
                                         output_dir=output_dir)

    @classmethod
    def casa_script(cls, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return cls._build_from_ps_oussid(basename,
                                         project_structure=project_structure,
                                         ousstatus_entity_id=ousstatus_entity_id,
                                         output_dir=output_dir)

    @classmethod
    def manifest(cls, basename, ousstatus_entity_id, output_dir=None):
        return cls._build_from_oussid(basename,
                                      ousstatus_entity_id=ousstatus_entity_id,
                                      output_dir=output_dir)

    @classmethod
    def calapply_list(cls, vis, aux_product=False, output_dir=None):
        basename = cls._build_calproduct_name('apply.txt', aux_product=aux_product)
        return cls._build_from_vis(basename, vis, output_dir=output_dir)

    @classmethod
    def caltables(cls, ousstatus_entity_id=None, session_name=None, aux_product=False, output_dir=None):
        basename = cls._build_calproduct_name('tables.tgz', aux_product=aux_product)
        return cls._build_from_oussid_session(basename=basename,
                                              ousstatus_entity_id=ousstatus_entity_id,
                                              session_name=session_name,
                                              output_dir=None)

    @classmethod
    def auxiliary_products(cls, basename, ousstatus_entity_id=None, output_dir=None):
        return cls._build_from_oussid(basename,
                                      ousstatus_entity_id=ousstatus_entity_id,
                                      output_dir=output_dir)

    @classmethod
    def aqua_report(cls, aqua_report_name, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return cls._build_from_ps_oussid(aqua_report_name,
                                         project_structure=project_structure,
                                         ousstatus_entity_id=ousstatus_entity_id,
                                         output_dir=output_dir)


if __name__ == '__main__':
    log = CASALog()
    print(log.get_filename())

    x = CalibrationTable(log).phase_only_gain_cal().spectral_window(4).polarization('Y')
    print(x.get_filename())

    x.polarization('X').spectral_window(3)
    print(x.get_filename())
