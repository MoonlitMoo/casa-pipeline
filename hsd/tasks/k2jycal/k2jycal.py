"""The k2jycal task to perform the calibration of Jy/K conversion."""
import copy
import os
import numpy as np
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.utils import relative_path
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as caltable_heuristic
from . import jyperkreader

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.infrastructure.callibrary import CalApplication

LOG = infrastructure.get_logger(__name__)

QUERIED_FACTOR_FILE = 'jyperk_query.csv'  # filename of the queried factor file


class SDK2JyCalInputs(vdp.StandardInputs):
    """Inputs class for SDK2JyCal task."""

    reffile = vdp.VisDependentProperty(default='jyperk.csv')
    dbservice = vdp.VisDependentProperty(default=True)
    endpoint = vdp.VisDependentProperty(default='asdm')
    caltype = vdp.VisDependentProperty(default='amp', readonly=True)

    @vdp.VisDependentProperty
    def infiles(self) -> str:
        """Return name of MS. Alias for "vis" attribute."""
        return self.vis

    @infiles.convert
    def infiles(self, value: Union[str, List[str]]) -> Union[str, List[str]]:
        """Convert value into expected type.

        Currently, no conversion is performed.

        Args:
            value: Name of MS, or the list of names

        Returns:
            Converted value. Currently return input value as is.
        """
        self.vis = value
        return value

    @vdp.VisDependentProperty
    def caltable(self):
        """Get the caltable argument for these inputs.

        If set to a table-naming heuristic, this should give a sensible name
        considering the current CASA task arguments.
        """
        namer = caltable_heuristic.AmpCaltable()
        # ignore caltable to avoid circular reference
        casa_args = self._get_task_args(ignore=('caltable',))
        return relative_path(namer.calculate(output_dir=self.output_dir,
                                             stage=self.context.stage,
                                             **casa_args))

    def to_casa_args(self) -> Dict[str, str]:
        """Convert Inputs instance into dictionary.

        Returns:
            kwargs for CASA task
        """
        return {'vis': self.vis,
                'caltable': self.caltable,
                'caltype': self.caltype,
                'endpoint': self.endpoint}

    def __init__(
        self,
        context: 'Context',
        output_dir: Optional[str] = None,
        infiles: Optional[Union[str, List[str]]] = None,
        caltable: Optional[Union[str, List[str]]] = None,
        reffile: Optional[str] = None,
        dbservice: Optional[bool] = None,
        endpoint: Optional[str] = None
    ) -> None:
        """Initialize SDK2JyCalInputs instance.

        Args:
            context: Pipeline context
            output_dir: Output directory. Defaults to None.
            infiles: Name of MS or list of names. Defaults to None.
            caltable: Name of caltable or list of names. Defaults to None.
                      Name is automatically created from infiles if None is given.
            reffile: Name of the file that stores Jy/K factors. Defaults to None.
                     Name is 'jyperk.csv' if None is given.
            dbservice: Access to Jy/K DB if True. Defaults to None.
                       None is interpreted as True.
            endpoint: Name of the DB endpoint. Defaults to None.
                      Endpoint is 'asdm' if None is given.
        """
        super(SDK2JyCalInputs, self).__init__()

        # context and vis/infiles must be set first so that properties that require
        # domain objects can be function
        self.context = context
        self.infiles = infiles
        self.output_dir = output_dir

        # set the properties to the values given as input arguments
        self.caltable = caltable
        self.reffile = reffile
        self.dbservice = dbservice
        self.endpoint = endpoint


class SDK2JyCalResults(basetask.Results):
    """Class to hold processing result of SDK2JyCal task."""

    def __init__(
        self,
        vis: Optional[str] = None,
        final: List['CalApplication'] = [],
        pool: Any = [],
        reffile: Optional[str] = None,
        factors: Dict[str, Dict[int, Dict[str, Dict[str, float]]]] = {},
        all_ok: bool = False,
        dbstatus: Optional[bool] = None
    ) -> None:
        """Initialize SDK2JyCalResults instance.

        Args:
            vis: Name of MS. Defaults to None.
            final: List of final CalApplication instances. Defaults to [].
            pool: List of all CalApplication instances. Defaults to [].
            reffile: Name of Jy/K factor file. Defaults to None.
            factors: Dictionary of Jy/K factors. Defaults to {}.
            all_ok: Boolean flag for availability of factors. Defaults to False.
            dbstatus: Status of DB access. Defaults to None.
        """
        super(SDK2JyCalResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.error = set()
        self.reffile = reffile
        self.factors = factors
        self.all_ok = all_ok
        self.dbstatus = dbstatus

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        Merge of the result instance of Jy/K calibration task includes
        the following updates to Pipeline context,

          - register CalApplication instances to callibrary
          - register Jy/K conversion factors to MS domain object

        Args:
            context: Pipeline context
        """
        if not self.final:
            LOG.error('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)
        # merge k2jy factor to context assing the value as an attribute of MS
        for vis, valid_k2jy in self.factors.items():
            msobj = context.observing_run.get_ms(name=vis)
            msobj.k2jy_factor = {}
            for spwid, spw_k2jy in valid_k2jy.items():
                for ant, ant_k2jy in spw_k2jy.items():
                    for pol, pol_k2jy in ant_k2jy.items():
                        msobj.k2jy_factor[(spwid, ant, pol)] = pol_k2jy

    def __repr__(self) -> str:
        """Return string representation of the instance."""
        # Format the Tsyscal results.
        s = 'SDK2JyCalResults:\n'
        for calapplication in self.final:
            s += '\tBest caltable for spw #{spw} in {vis} is {name}\n'.format(
                spw=calapplication.spw, vis=os.path.basename(calapplication.vis),
                name=calapplication.gaintable)
        return s


@task_registry.set_equivalent_casa_task('hsd_k2jycal')
@task_registry.set_casa_commands_comment('The Kelvin to Jy calibration tables are generated.')
class SDK2JyCal(basetask.StandardTaskTemplate):
    """Generate calibration table of Jy/K factors."""

    Inputs = SDK2JyCalInputs

    def execute( self, **parameters) -> SDK2JyCalResults:
        """
        remove existing QUERIED_FACTOR_FILE before the first run

        Args:
            parameters: parameters
        Returns
            SDK2JyCalResults
        """
        filename = QUERIED_FACTOR_FILE
        if self.inputs.context.subtask_counter == 0:
            if os.path.isfile(filename):
                LOG.info( "File {} exists, will rename to {}_orig".format(filename, filename) )
                if os.path.isfile(filename+"_orig"):
                    LOG.info( "Existing {}_orig will be overwritten".format(filename) )
                os.rename( filename, "{}_orig".format(filename) )

        results = super().execute( **parameters )
        return results

    def prepare(self) -> SDK2JyCalResults:
        """
        Retrieve Jy/K facors from the DB and save them in QUERIED_FACTOR_FILE if dbaccess is True

        Returns:
            SDK2JyCalResults
        """
        inputs = self.inputs
        vis = inputs.vis
        reffile = inputs.reffile
        caltable_status = None

        if not os.path.exists(vis):
            LOG.error( "Could not find MS '{}'".format(vis) )
            return SDK2JyCalResults(os.path.basename(vis))
        vis = os.path.basename(vis)

        # make a note of the current inputs state before we start fiddling
        # with it. This origin will be attached to the final CalApplication.
        origin = callibrary.CalAppOrigin(task=SDK2JyCal,
                                         inputs=inputs.to_casa_args())
        common_params = inputs.to_casa_args()

        # create caltable and extract data for pipeline
        caltable_status = self._create_caltable(common_params)
        if caltable_status is False:
            LOG.error("No Jy/K scaling factors available")
            return SDK2JyCalResults(os.path.basename(vis))
        factors_used = self._extract_factors( inputs.context, vis, common_params['caltable'], caltable_status )
        if factors_used is None:
            LOG.error("MS and caltable are inconsistent")
            return SDK2JyCalResults(os.path.basename(vis))

        # write jyperk data to file if fetched from DB
        if caltable_status is True:
            reffile = QUERIED_FACTOR_FILE
            export_jyperk( reffile, vis, factors_used )

        # generate callibrary for the caltable
        callist = []
        valid_factors = {}
        calto = callibrary.CalTo(vis=common_params['vis'])
        calfrom = callibrary.CalFrom(common_params['caltable'],
                                     caltype=inputs.caltype,
                                     gainfield='', spwmap=None,
                                     interp='nearest,nearest')
        calapp = callibrary.CalApplication(calto, calfrom, origin)
        if calapp is not None:
            callist = [calapp]
            factors_ok = True
        else:
            callist = []
            factors_ok = False
        valid_factors[vis] = factors_used

        return SDK2JyCalResults(vis=vis, pool=callist, reffile=reffile,
                                factors=valid_factors, all_ok=factors_ok,
                                dbstatus=caltable_status)

    def _extract_factors( self, context: 'Context', vis: str, caltable: str, dbstatus: bool ) -> Optional[Dict[str, Dict[str, Dict[str, float]]]]:
        """
        extract Jy/K factors

        Args:
            context  : Pipeline context
            vis      : Name of MS
            caltable : Name of caltable
            dbstatus : status of DB service
        Returns:
            Jy/K factors / None if MS and caltable are inconsistent
        """
        # get list of antennas and science_windows from ms
        ms = context.observing_run.get_ms(vis)
        antennas = [ x.name for x in ms.get_antenna() ]
        science_windows = [ x.id for x in ms.get_spectral_windows(science_windows_only=True) ]

        # get antenna list from caltable
        with casa_tools.TableReader(caltable+"/ANTENNA") as tb:
            caltable_antlist = tb.getcol("NAME")
        antidx = {}   # index of antenna in caltable
        for ant in antennas:
            if ant not in caltable_antlist:
                LOG.error( "{}: antenna {} does not exist in caltable".format(vis, ant) )
                return None
            antidx[ant] = np.where( caltable_antlist == ant )[0][0]

        # fetch Jy/K factors from caltable file
        factors_table = {}
        with casa_tools.TableReader(caltable) as tb:
            for spw in science_windows:
                factors_table[spw] = {}
                ddid = ms.get_data_description(spw=spw)
                pol_list = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))
                for ant in antennas:
                    subtb = tb.query( "ANTENNA1={} && SPECTRAL_WINDOW_ID={}".format(antidx[ant], spw) )
                    factors = subtb.getcol("CPARAM")[:,0,0].real
                    subtb.close()
                    if len(factors) < len(pol_list):
                        LOG.error( "{}: insufficient pols in caltable (MS:{} caltable:{})".format(vis, len(pol_list), len(factors)) )
                        return None
                    else:
                        factors_table[spw][ant] = {}
                        for polid, pol in enumerate(pol_list):
                            factor = factors[polid]
                            factors_table[spw][ant][pol] = 1.0/(factor*factor)

        # remove parameters not found in reffile
        # doing this because gencal() gives 1.0 for factors not found in reffile
        factors_used = copy.deepcopy(factors_table)
        if dbstatus is None:
            factors_list = jyperkreader.read(context, self.inputs.reffile)
            # scan through data
            for spw in factors_table.keys():
                for ant in factors_table[spw].keys():
                    for pol in factors_table[spw][ant].keys():
                        found = False
                        allowed_pol = [ pol, 'I' ]
                        for factor in factors_list:
                            if factor[1:3] == [ant, str(spw)] and factor[3] in allowed_pol:
                                found = True
                                break
                        if not found:
                            del factors_used[spw][ant][pol]
                    if factors_used[spw][ant] == {}:
                        del factors_used[spw][ant]

        return factors_used

    def _create_caltable( self, common_params: Dict[str, str] ) -> Optional[bool]:
        """
        Invoke gencal and ceate the calibration table file

        if inputs.dbservie is True, then try to obtain Jy/K factors from DB,
        but falls back to read jyper.csv if DB access fails.

        Args:
            common_params : common parameters for calibration
        Returns:
            status of creating caltable file.
                status = True  : Caltable is created from DB
                status = None  : Caltable is created from Jy/K factor CSV file
                status = False : Failed to create caltable
        """
        inputs = self.inputs
        status = None

        gencal_args = common_params.copy()
        gencal_args['caltype'] = 'jyperk'     # override to invoke gencal in jyperk mode

        # retrieve factors from DB
        if inputs.dbservice:
            gencal_job = casa_tasks.gencal(**gencal_args)
            try:
                self._executor.execute(gencal_job)
                status = True
            except Exception as e:
                if len(str(e)) == 0:
                    LOG.warning( "Failed to get Jy/K factors from DB." )
                else:
                    LOG.warning( e )
                LOG.warning( "{}: Query to Jy/K DB failed. Will fallback to read CSV file '{}'".format(inputs.vis, inputs.reffile) )
                status = False

        # retrieve factors from file
        if not status:
            gencal_args['infile'] = inputs.reffile

            if not os.path.exists(inputs.reffile):
                LOG.error( "Jy/K scaling factor file '{}' does not exist.".format(inputs.reffile) )
                status = False
            else:
                gencal_job = casa_tasks.gencal(**gencal_args)
                try:
                    self._executor.execute(gencal_job)
                    status = None
                except Exception as e:
                    LOG.error( "{}: Failed to create caltable from CSV file: {}".format(inputs.vis, e) )
                    status = False
        return status

    def analyse(self, result: SDK2JyCalResults) -> SDK2JyCalResults:
        """Analyse SDK2JyCalResults instance produced by prepare.

        1. Define factors actually used and analyze if the factors are provided to all relevant data in MS.
        2. Check if caltables in the pool exist to validate the CalApplication, and register valid CalApplication's to final
        attribute.

        Args:
            result: SDK2JyCalResults instance

        Returns:
            Updated SDK2JyCalResults instance
        """
        vis = result.vis
        if vis not in result.factors.keys() or len(result.factors[vis]) == 0:
            result.all_ok = False
            LOG.warning( "No Jy/K factor is given for MS '{}'".format(vis) )
            return result

        # check if factors are provided to all relevant data in MS
        ms = self.inputs.context.observing_run.get_ms(vis)
        pol_to_map_i = ('XX', 'YY', 'RR', 'LL', 'I')
        for spw in ms.get_spectral_windows(science_windows_only=True):
            spwid = spw.id
            if spwid not in result.factors[vis]:
                result.all_ok = False
                LOG.warning( "No Jy/K factor is given for Spw={} of {}".format( spwid, vis ) )
                continue
            ddid = ms.get_data_description(spw=spwid)
            pol_list = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))
            # mapping for anonymous antenna if necessary
            all_ant_factor = result.factors[vis][spwid].pop('ANONYMOUS', {})
            for ant in ms.get_antenna():
                ant_name = ant.name
                if all_ant_factor:
                    result.factors[vis][spwid][ant_name] = all_ant_factor
                elif ant_name not in result.factors[vis][spwid]:
                    result.all_ok = False
                    LOG.warning("No Jy/K factor is given for Spw={}, Ant={} of {}".format(spwid, ant_name, vis))
                    continue
                all_pol_factor = result.factors[vis][spwid][ant_name].pop('I', {})
                for pol in pol_list:
                    # mapping for stokes I if necessary
                    if all_pol_factor and pol in pol_to_map_i:
                        result.factors[vis][spwid][ant_name][pol] = all_pol_factor
                    # check factors provided for all spw, antenna, and pol
                    ok = self.__check_factor(result.factors[vis], spwid, ant_name, pol)
                    result.all_ok &= ok
                    if not ok:
                        LOG.warning("No Jy/K factor is given for Spw={}, Ant={}, Pol={} of {}".format(spwid, ant_name, pol, vis))

        # double-check that the caltable was actually generated and prepare 'final'.
        on_disk = [ca for ca in result.pool if ca.exists()]
        result.final[:] = on_disk

        missing = [ca for ca in result.pool if ca not in on_disk]
        result.error.clear()
        result.error.update(missing)

        return result

    @staticmethod
    def __check_factor(
        factors: Dict[int, Dict[str, Dict[str, float]]],
        spw: int,
        ant: str,
        pol: str
    ) -> bool:
        """Check if factor for given meta data is available
        Args:
            factors: List of Jy/K factors with meta data
            spw: Spectral window id
            ant: Antenna name
            pol: Polarization type
        Returns:
            Availability of the factor for given meta data
        """
        if factors.get( spw, None ) is None:
            return False
        if factors[spw].get( ant, None ) is None:
            return False
        if factors[spw][ant].get( pol, None ) is None:
            return False
        return True


def export_jyperk( outfile: str, vis: str, factors_used: dict ) -> None:
    """Export conversion factors to file.

    Format of the output file is CSV.

    Args:
        outfile : Name of the output file
        vis     : Name of MS
        factors : List of conversion factors with meta data
    """
    if not os.path.exists(outfile):
        # create file with header information
        with open(outfile, 'w') as f:
            f.write('MS,Antenna,Spwid,Polarization,Factor\n')

    with open(outfile, 'a') as f:
        for spw, v1 in factors_used.items():
            for ant, v2 in v1.items():
                for pol, factor in v2.items():
                    f.write( "{},{},{},{},{}\n".format( vis, ant, spw, pol, factor ) )
