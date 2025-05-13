from typing import TYPE_CHECKING, List, Optional, Union

import pipeline.h.tasks.tsysflag.tsysflag as tsysflag
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.sessionutils as sessionutils
from pipeline.infrastructure import task_registry

if TYPE_CHECKING:
    from numbers import Integral

    from pipeline.infrastructure import Context

__all__ = [
    'Tsysflag',
    'TsysflagInputs'
]

LOG = infrastructure.get_logger(__name__)


class TsysflagInputs(tsysflag.TsysflagInputs):
    """
    TsysflagInputs defines the inputs for the Tsysflag pipeline task.
    """
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self,
                 context: 'Context',
                 output_dir: Optional[str] = None,
                 vis: Optional[List[str]] = None,
                 caltable: Optional[List[str]] = None,
                 flag_nmedian: Optional[Union[bool, str]] = None,
                 fnm_limit: Optional[Union['Integral', str]] = None,
                 fnm_byfield: Optional[Union[bool, str]] = None,
                 flag_derivative: Optional[Union[bool, str]] = None,
                 fd_max_limit: Optional[Union['Integral', str]] = None,
                 flag_edgechans: Optional[Union[bool, str]] = None,
                 fe_edge_limit: Optional[Union['Integral', str]] = None,
                 flag_fieldshape: Optional[Union[bool, str]] = None,
                 ff_refintent: Optional[str] = None,
                 ff_max_limit: Optional[Union['Integral', str]] = None,
                 flag_birdies: Optional[Union[bool, str]] = None,
                 fb_sharps_limit: Optional[Union['Integral', str]] = None,
                 flag_toomany: Optional[Union[bool, str]] = None,
                 tmf1_limit: Optional[Union['Integral', str]] = None,
                 tmef1_limit: Optional[Union['Integral', str]] = None,
                 metric_order: Optional[str] = None,
                 normalize_tsys: Optional[Union[bool, str]] = None,
                 filetemplate: Optional[str] = None,
                 parallel: Optional[Union[str, bool]] = None):
        """Construct TsysflagInputs instance for SD Tsysflag task.

        Args:
            context: Pipeline context.
            output_dir: Output directory.
            vis: List of MeasurementSets (not used).
            caltable: List of input Tsys calibration tables.
            flag_nmedian: True to flag Tsys spectra with high median value.
                          Defaults to True.
            fnm_limit: Flag spectra with median value higher than
                       fnm_limit * median of this measure over all spectra.
                       Defaults to 2.0.
            fnm_byfield: Evaluate the nmedian metric separately for each field.
                         Defaults to True.
            flag_derivative: True to flag Tsys spectra with high median
                             derivative. Defaults to True.
            fd_max_limit: Flag spectra with median derivative higher than
                          fd_max_limit * median of this measure over all
                          spectra. Defaults to 5.0.
            flag_edgechans: True to flag edges of Tsys spectra.
                            Defaults to True.
            fe_edge_limit: Flag channels whose channel to channel difference >
                           fe_edge_limit * median across spectrum.
                           Defaults to 3.0.
            flag_fieldshape: True to flag Tsys spectra with a radically
                             different shape to those of the ff_refintent.
                             Defaults to True.
            ff_refintent: Data intent that provides the reference shape
                          for 'flag_fieldshape'. Defaults to 'BANDPASS'.
            ff_max_limit: Flag Tsys spectra with 'fieldshape' metric values >
                          ff_max_limit. Defaults to 13.
            flag_birdies: True to flag channels covering sharp spectral features.
                          Defaults to True.
            fb_sharps_limit: Flag channels bracketing a channel to channel
                             difference > fb_sharps_limit. Defualts to 0.15.
            flag_toomany: True to flag Tsys spectra for which a proportion of
                          antennas for given timestamp and/or proportion of
                          antennas that are entirely flagged in all timestamps
                          exceeds their respective thresholds.
                          Defaults to True.
            tmf1_limit: Flag Tsys spectra for all antennas in a timestamp
                        and spw if proportion of antennas already flagged
                        in this timestamp and spw exceeds tmf1_limit.
                        Defaults to 0.666.
            tmef1_limit: Flag Tsys spectra for all antennas and all timestamps
                         in a spw, if proportion of antennas that are already
                         entirely flagged in all timestamps exceeds tmef1_limit.
                         Defaults to 0.666.
            metric_order: Order in which to evaluate the flagging metrics
                          that are enables. Disabled metrics are skipped.
                          Default order is as follows:
                          nmedian derivative edgechans fieldshape birdies toomany
            normalize_tsys: True to create a normalized Tsys table that is used to
                            evaluate the Tsys flagging metrics. All newly found
                            flags are also applied to the original Tsys caltable
                            that continues to be used for subsequent calibration.
                            Defaults to False.
            filetemplate: The name of a text file that contains the manual Tsys
                          flagging template. If the template flags file is
                          undefined, a name of the form 'msname.flagtsystemplate.txt'
                          is assumed.
            parallel: Execute using CASA HPC functionality, if available.
                      Default is None, which intends to turn on parallel
                      processing if possible.
        """
        super().__init__(
            context=context, output_dir=output_dir, vis=vis, caltable=caltable,
            flag_nmedian=flag_nmedian, fnm_limit=fnm_limit, fnm_byfield=fnm_byfield,
            flag_derivative=flag_derivative, fd_max_limit=fd_max_limit,
            flag_edgechans=flag_edgechans, fe_edge_limit=fe_edge_limit,
            flag_fieldshape=flag_fieldshape, ff_refintent=ff_refintent, ff_max_limit=ff_max_limit,
            flag_birdies=flag_birdies, fb_sharps_limit=fb_sharps_limit,
            flag_toomany=flag_toomany, tmf1_limit=tmf1_limit, tmef1_limit=tmef1_limit,
            metric_order=metric_order, normalize_tsys=normalize_tsys, filetemplate=filetemplate)

        self.parallel = parallel


@task_registry.set_equivalent_casa_task('hsd_tsysflag')
@task_registry.set_casa_commands_comment('The Tsys calibration and spectral window map is computed.')
class SerialTsysflag(tsysflag.Tsysflag):
    Inputs = TsysflagInputs


class Tsysflag(sessionutils.ParallelTemplate):
    Inputs = TsysflagInputs
    Task = SerialTsysflag
