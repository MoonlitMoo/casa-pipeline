"""Classes and methods to create SD Flag Plots."""
import os
from typing import Dict, List, Optional, Tuple

import matplotlib  
import matplotlib.figure as figure 
from matplotlib.axes._axes import Axes as MplAxes

from pipeline.domain import DataTable, MeasurementSet
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from . import SDFlagRule
from .SDFlagRule import INVALID_STAT
from ..common import display as sd_display
from ..common import utils as sdutils

LOG = infrastructure.get_logger(__name__)

DPIDetail = 130
FIGSIZE_INCHES = (7.0, 2.9)

class SDFlagPlotter(object):
    """Class to create Flag Plots for hsd_blflag weblog."""

    def __init__( self, msobj:MeasurementSet, datatable:DataTable, antid:int, spwid:int,
                  time_gap:List[List[int]], FigFileDir:Optional[str] ):
        """
        Construct SDFlagPlotter instance.

        Args:
            msobj          : Measurementset object
            datatable      : DataTable
            antid          : antenna ID
            spwid          : SpW ID
            time_gap       : time gap
            FigFileDir     : directory to output figure files
        Returns:
            (none)
        """
        self.msobj = msobj
        self.datatable = datatable
        self.antid = antid
        self.spwid = spwid
        self.time_gap = time_gap
        self.FigFileDir = FigFileDir

        self.pollist = []
        self.is_baselined_dict   = {}
        self.FlagRule_local_dict = {}
        self.PermanentFlag_dict  = {}
        self.NPp_dict            = {}
        self.threshold_dict      = {}

        return


    def register_data( self, pol:str,
                         is_baselined:bool, FlagRule_local:Dict,
                         PermanentFlag:List[int], NPp:Dict,
                         threshold:List[List[float]] ) -> List[str]:
        """
        Resiter data to be plotted.

        Args:
            pol            : polarization
            is_baselined   : True if baselined, False if not
            FlagRule_local : FlagRule
            PermanentFlag  : permanent Flag
            NPp            : flagging data summarized for weblog
            threshold      : threshold
        Returns:
            (none)
        Raises:
            RuntimeError if attempted to register data for existing pol
        """
        if pol in self.pollist:
            raise RuntimeError( "Attempted to register existing pol={} in {}".format(pol, self.pollist))
        self.pollist.append(pol)
        self.is_baselined_dict[pol]   = is_baselined
        self.FlagRule_local_dict[pol] = FlagRule_local
        self.PermanentFlag_dict[pol]  = PermanentFlag
        self.NPp_dict[pol]            = NPp
        self.threshold_dict[pol]      = threshold

        return


    def create_plots( self, FigFileRoot:Optional[str]=None ) -> Dict:
        """
        Create Summary plots.

        Args:
            FigFileRoot : basename of figure files
        Returns:
            List of dictonaries of plot data
                'file' : List of Figure filenames (basename only)
                'type' : Type string for selector
        """
        msobj = self.msobj
        datatable = self.datatable
        antid = self.antid
        spwid = self.spwid
        time_gap = self.time_gap
        FigFileDir = self.FigFileDir

        pollist = self.pollist
        is_baselined_dict = self.is_baselined_dict
        FlagRule_local_dict = self.FlagRule_local_dict
        PermanentFlag_dict = self.PermanentFlag_dict
        NPp_dict = self.NPp_dict
        threshold_dict = self.threshold_dict

        ant_name = msobj.get_antenna(antid)[0].name
        bunit = sdutils.get_brightness_unit( msobj.name, defaultunit='Jy/beam' )
        PosGap = time_gap[0]
        TimeGap = time_gap[1]
        plots = []

        # prepare PlotData
        PlotData_dict = {}
        for pol in pollist:
            PlotData_dict[pol] = {
                'ms_name': msobj.name,
                'ant_name' : ant_name,
                'spw' : spwid,
                'pol' : pol
            }

        flag_desc = SDFlagRule.SDFlag_Desc
        # Tsys flag
        timeCol = datatable.getcol('TIME')
        PosGapInTime = timeCol.take(PosGap)
        TimeGapInTime = timeCol.take(TimeGap)
        for pol in pollist:
            PlotData_dict[pol]['row'] =  NPp_dict[pol]['rows']['TsysFlag']
            PlotData_dict[pol]['time'] = NPp_dict[pol]['time']['TsysFlag']
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['TsysFlag']
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['TsysFlag']
            PlotData_dict[pol]['thre'] = [threshold_dict[pol][4][1], 0.0]
            PlotData_dict[pol]['gap'] =  [PosGapInTime, TimeGapInTime]
            PlotData_dict[pol]['title'] = "Tsys (K)"
            PlotData_dict[pol]['xlabel'] = "Time (UTC)"
            PlotData_dict[pol]['ylabel'] = "Tsys (K)"
            PlotData_dict[pol]['permanentflag'] = PermanentFlag_dict[pol]
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['TsysFlag']['isActive']
            PlotData_dict[pol]['threType'] = "line"
            PlotData_dict[pol]['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local_dict[pol]['TsysFlag']['Threshold'])
        figfilename = FigFileRoot + '_0.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['TsysFlag'] } )

        # RMS flag before baseline fit
        for pol in pollist:
            PlotData_dict[pol]['row'] = NPp_dict[pol]['rows']['BaselineFlag']
            PlotData_dict[pol]['time'] = NPp_dict[pol]['time']['BaselineFlag']
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RmsPreFitFlag']
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RmsPreFitFlag']
            PlotData_dict[pol]['thre'] = [threshold_dict[pol][1][1]]
            PlotData_dict[pol]['title'] = "Baseline RMS ({}) before baseline subtraction".format(bunit)
            PlotData_dict[pol]['ylabel'] = "Baseline RMS ({})".format(bunit)
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RmsPreFitFlag']['isActive']
            PlotData_dict[pol]['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local_dict[pol]['RmsPreFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_1.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['RmsPreFitFlag'] } )

        # RMS flag after baseline fit
        for pol in pollist:
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RmsPostFitFlag'] if is_baselined_dict[pol] else None
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RmsPostFitFlag']
            PlotData_dict[pol]['thre'] = [threshold_dict[pol][0][1]]
            PlotData_dict[pol]['title'] = "Baseline RMS ({}) after baseline subtraction".format(bunit)
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RmsPostFitFlag']['isActive']
            PlotData_dict[pol]['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local_dict[pol]['RmsPostFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_2.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['RmsPostFitFlag'] } )

        # Running mean flag before baseline fit
        for pol in pollist:
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RunMeanPreFitFlag']
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RunMeanPreFitFlag']
            PlotData_dict[pol]['thre'] = [threshold_dict[pol][3][1]]
            PlotData_dict[pol]['title'] = "RMS ({}) for Baseline Deviation from the running mean (Nmean={:d}) before baseline subtraction".format(bunit, FlagRule_local_dict[pol]['RunMeanPreFitFlag']['Nmean'])
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RunMeanPreFitFlag']['isActive']
            PlotData_dict[pol]['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local_dict[pol]['RunMeanPreFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_3.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['RunMeanPreFitFlag'] } )

        # Running mean flag after baseline fit
        for pol in pollist:
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RunMeanPostFitFlag'] if is_baselined_dict[pol] else None
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RunMeanPostFitFlag']
            PlotData_dict[pol]['thre'] = [threshold_dict[pol][2][1]]
            PlotData_dict[pol]['title'] = "RMS ({}) for Baseline Deviation from the running mean (Nmean={:d}) after baseline subtraction".format(bunit, FlagRule_local_dict[pol]['RunMeanPostFitFlag']['Nmean'])
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RunMeanPostFitFlag']['isActive']
            PlotData_dict[pol]['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local_dict[pol]['RunMeanPostFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_4.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename)
        plots.append( { 'file':figfilename, 'type':flag_desc['RunMeanPostFitFlag'] } )

        # Expected RMS flag before baseline fit
        for pol in pollist:
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RmsPreFitFlag']
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RmsExpectedPreFitFlag']
            PlotData_dict[pol]['thre'] = [NPp_dict[pol]['data']['RmsExpectedPreFitFlag']]
            PlotData_dict[pol]['title'] = "Baseline RMS ({}) compared with the expected RMS calculated from Tsys before baseline subtraction".format(bunit)
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RmsExpectedPreFitFlag']['isActive']
            PlotData_dict[pol]['threType'] = "plot"
            PlotData_dict[pol]['threDesc'] = "threshold with scaling factor = {:.1f}".format(FlagRule_local_dict[pol]['RmsExpectedPreFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_5.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['RmsExpectedPreFitFlag'] } )

        # Expected RMS flag after baseline fit
        for pol in pollist:
            PlotData_dict[pol]['data'] = NPp_dict[pol]['data']['RmsPostFitFlag'] if is_baselined_dict[pol] else None
            PlotData_dict[pol]['flag'] = NPp_dict[pol]['flag']['RmsExpectedPostFitFlag']
            PlotData_dict[pol]['thre'] = [NPp_dict[pol]['data']['RmsExpectedPostFitFlag']]
            PlotData_dict[pol]['title'] = "Baseline RMS ({}) compared with the expected RMS calculated from Tsys after baseline subtraction".format(bunit)
            PlotData_dict[pol]['isActive'] = FlagRule_local_dict[pol]['RmsExpectedPostFitFlag']['isActive']
            PlotData_dict[pol]['threType'] = "plot"
            PlotData_dict[pol]['threDesc'] = "threshold with scaling factor = {:.1f}".format(FlagRule_local_dict[pol]['RmsExpectedPostFitFlag']['Threshold'])
        figfilename = FigFileRoot + '_6.png'
        self.StatisticsPlot( pollist, PlotData_dict, FigFileDir, figfilename )
        plots.append( { 'file':figfilename, 'type':flag_desc['RmsExpectedPostFitFlag'] } )

        return plots


    @casa5style_plot
    def StatisticsPlot( self, pollist:List[str], PlotData_dict:Dict, FigFileDir:Optional[str]=None, figfilename:Optional[Dict]=None ):
        """
        Create blflag statistics plot.

        Args:
            pollist         : list of pols
            PlotData_dict   : dictonary of PlotData
                                PlotData_dict[pol] = {
                                    ms_name: MeasurementSet name
                                    ant_name:  Antenna name
                                    spw:  spwid
                                    pol:  polarization
                                    row:  [row], # row number of the spectrum
                                    time: [time], # timestamp
                                    data: [data],
                                    flag: [flag], # 0: flag out, 1: normal, 2: exclude from the plot
                                    sigma: "Clipping Sigma"
                                    thre: [threshold(s) max, min(if any)], # holizontal line
                                    gap:  [gap(s)], # vertical tick: [[PosGap], [TimeGap]]
                                    title: "title",
                                    xlabel: "xlabel",
                                    ylabel: "ylabel"
                                    permanentflag: [PermanentFlag rows]
                                    isActive: True/False
                                    threType: "line" or "plot" # if "plot" then thre should be a list
                                       having the equal length of row
                                    threDesc: description of the threshold (for legend)
                                }
            FigFileDir      : directory to create figure files
            figfilename     : figure filename
        Returns:
            (none)
        Raises:
            RuntimeError if number of pols exceeds the limit of 4
        """
        if len(pollist)>4:   # max number of pols=4
            raise RuntimeError( "Number of polarizations ({}) exceeds limit of 4.".format(len(pollist)) )
        if FigFileDir is None:
            return

        # create listofplots.txt
        if os.access(FigFileDir+'listofplots.txt', os.F_OK):
            BrowserFile = open(FigFileDir+'listofplots.txt', 'a')
        else:
            BrowserFile = open(FigFileDir+'listofplots.txt', 'w')
            print('TITLE: BF_Stat', file=BrowserFile)
            print('FIELDS: Stat IF POL Iteration Page', file=BrowserFile)
            print('COMMENT: Statistics of spectra', file=BrowserFile)
        print( figfilename, file=BrowserFile)
        BrowserFile.close()

        # pack data into working vars
        data = {}
        xlim = {}
        ylim = {}
        ScaleOut = {}
        LowRange = {}
        for pol in pollist:
            data[pol], xlim[pol], ylim[pol], ScaleOut[pol], LowRange[pol] = self._pack_data( PlotData_dict[pol] )
        # do the plots!
        self._plot( FigFileDir, figfilename, pollist, PlotData_dict, data, xlim, ylim, ScaleOut, LowRange )

        return


    def _pack_data( self, plotdata:Dict ) -> Tuple[ Dict, List[float], List[float], Dict, Dict ]:
        """
        Pack data for plotting for each pol.

        Args:
            plotdata: dictionary of PlotData
        Returns:
            data:     dictionary of plotting data
            xlim:     x-limits calculated
            ylim:     y-limits calculated
            ScaleOut: dictionary of ScaleOut
            LowRange: dictionary of LowRange
        """
        if plotdata['data'] is not None:
            if len(plotdata['thre']) > 1:
                LowRange = True
            else:
                LowRange = False
                if plotdata['threType'] != "plot":
                    plotdata['thre'].append(min(plotdata['data']))
                else:
                    plotdata['thre'].append(min(min(plotdata['data']), min(plotdata['thre'][0])))

            # Calculate X-scale
            xlim = [ min(plotdata['time']), max(plotdata['time']) ]

            # Calculate Y-scale
            if plotdata['threType'] == "plot":
                yrange = max(plotdata['thre'][0]) - plotdata['thre'][1]
                if yrange == 0.0:
                    yrange = 1.0
                ymin = max(0.0, plotdata['thre'][1] - yrange * 0.5)
                ymax = (max(plotdata['thre'][0]) - ymin) * 1.3333333333 + ymin
            else:
                yrange = plotdata['thre'][0] - plotdata['thre'][1]
                if yrange == 0.0:
                    yrange = 1.0
                ymin = max(0.0, plotdata['thre'][1] - yrange * 0.5)
                ymax = (plotdata['thre'][0] - ymin) * 1.3333333333 + ymin
            ylim = [ ymin, ymax ]
            yy = ymax - ymin
            ScaleOut = [[ymax - yy * 0.1, ymax - yy * 0.04],
                        [ymin + yy * 0.1, ymin + yy * 0.04]]

            # Make Plot Data
            data = {
                'online_x'    : [],
                'online_y'    : [],
                'deviator_x'  : [],
                'deviator_y'  : [],
                'normal_x'    : [],
                'normal_y'    : []
            }
            for x, Pflag in enumerate(plotdata['permanentflag']):
                if Pflag == 0 or plotdata['data'][x] == INVALID_STAT:  # Flag-out case
                    data['online_x'].append(plotdata['time'][x])
                    if plotdata['data'][x] > ScaleOut[0][0] or plotdata['data'][x] == INVALID_STAT:
                        data['online_y'].append(ScaleOut[0][1])
                    elif LowRange and plotdata['data'][x] < ScaleOut[1][0]:
                        data['online_y'].append(ScaleOut[1][1])
                    else:
                        data['online_y'].append(plotdata['data'][x])
                elif plotdata['flag'][x] == 0:  # Flag-out case
                    data['deviator_x'].append(plotdata['time'][x])
                    if plotdata['data'][x] > ScaleOut[0][0]:
                        data['deviator_y'].append(ScaleOut[0][1])
                    elif LowRange and plotdata['data'][x] < ScaleOut[1][0]:
                        data['deviator_y'].append(ScaleOut[1][1])
                    else:
                        data['deviator_y'].append(plotdata['data'][x])
                else:  # Normal case
                    data['normal_x'].append(plotdata['time'][x])
                    data['normal_y'].append(plotdata['data'][x])
        else: # "NO DATA" case
            xlim = None
            ylim = None
            data = None
            ScaleOut = None
            LowRange = None

        return data, xlim, ylim, ScaleOut, LowRange


    def _plot( self, figfiledir:Optional[str], figfilename:Optional[str],
               pollist:List[int],
               PlotData_dict:Dict, data_dict:Optional[Dict],
               xlim_dict:List[Dict], ylim_dict:List[Dict],
               ScaleOut_dict:Optional[Dict], LowRange_dict:Optional[Dict] ):
        """
        Create actual plots.

        Args:
            figfiledir    : directory to write the figure file
            figfilename   : filename of the figure file
            pollist       : list of polarization
            PlotData_dict : dictionary of PlotData (for all pols)
            data_dict     : dictionary of plotting data (for all pols)
            xlim_dict     : dictionary of x-limits (for all pols)
            ylim_dict     : dictionary of y-limits (for all pols)
            ScaleOut_dict : dictionary of Scaleout (for all pols)
            LowRange_dict : dictionary of LowRange (for all pols)
        Returns:
            (none)
        """
        # do nothing if no plots are going to be saved on file
        if figfiledir is None or figfilename is None:
            return

        # initial settings & hold original plot configurations
        fig = figure.Figure( figsize=FIGSIZE_INCHES )

        # pick the widest limits
        xlim_values = [ x for x in xlim_dict.values() if x ]
        ylim_values = [ y for y in ylim_dict.values() if y ]
        global_xlim = [ min(v[0] for v in xlim_values), max(v[1] for v in xlim_values) ] if len(xlim_values)>0 else [-1,1]
        global_ylim = [ min(v[0] for v in ylim_values), max(v[1] for v in ylim_values) ] if len(ylim_values)>0 else [-1,1]

        # create plots for each pol on individual axes
        ax = {}
        npol = len( pollist )
        for pol in pollist:
            ax[pol] = fig.add_axes( [0.1, 0.13, 0.88, 0.72-0.04*(npol-1)], label=pol )
        self.__plot_frame_to_axes( pollist, ax, PlotData_dict, global_xlim, global_ylim )
        for pol in pollist:
            self.__plot_data_to_axes( pollist, ax[pol],
                                      PlotData_dict[pol], data_dict[pol], ScaleOut_dict[pol], LowRange_dict[pol] )

        # save the entire plot to png file
        outfile = figfiledir + figfilename
        fig.savefig( outfile, dpi=DPIDetail )

        return


    def __plot_frame_to_axes( self, pollist, ax:Dict, PlotData_dict:Dict,
                              xlim:List[float], ylim:List[float] ):
        """
        Prepare the frame on axes.

        Args:
            pollist:  list of pols
            ax:       axes to plot
            Plotdata: PlotData to plot
            xlim:     xlimits
            ylim:     ylimits
        Returns:
            (none)
        """
        # check consistency
        for pol in pollist:
            if PlotData_dict[pol]['data'] is None and PlotData_dict[pol]['isActive']:
                LOG.info( "No statistics data to show for Active flags: ant:{} spw:{} pol:{} \"{}\"".format(
                    PlotData_dict[pol]['ant_name'],
                    PlotData_dict[pol]['spw'],
                    pol,
                    PlotData_dict[pol]['title']
                ) )

        # loop over pols
        for pol in pollist:
            # X-axis label format
            xmin, xmax = sd_display.mjd_to_plotval( xlim )
            ax[pol].axis( [xmin, xmax, ylim[0], ylim[1]] )
            ax[pol].xaxis.set_major_locator(sd_display.utc_locator(start_time=xlim[0], end_time=xlim[1]))
            ax[pol].xaxis.set_major_formatter(sd_display.utc_formatter())
            ax[pol].axis( "off" )   # skip drawing frame

        # find the first pol with data
        pol0 = None
        for pol in pollist:
            if PlotData_dict[pol]['data'] is not None:
                pol0 = pol
                break

        # pick one axes (=axf) to draw frame and header
        polf = pollist[0] if pol0 is None else pol0
        axf = ax[polf]
        axf.axis( "on" )
        # determine the position of title and Active flag with respect to axes size
        axpos = axf.get_position()
        loc_y = 1.07 * 0.72/(axpos.y1 - axpos.y0)
        title_text = "{}\nMS: {}    Antenna: {}    SpW: {}".format(
            PlotData_dict[polf]['title'], PlotData_dict[polf]['ms_name'], PlotData_dict[polf]['ant_name'], PlotData_dict[polf]['spw'])
        # pol info goes to title when there are no overplottings
        if len(pollist) == 1:
            title_text = title_text + "    Pol: {}".format( PlotData_dict[polf]['pol'] )
        axf.set_title( title_text, fontsize=7, horizontalalignment='center', y=loc_y )
        axf.set_xlabel( PlotData_dict[pol]['xlabel'], fontsize=6 )
        axf.set_ylabel( PlotData_dict[pol]['ylabel'], fontsize=7 )
        axf.tick_params( axis='both', labelsize=6 )
        # show ACTIVE/INACTIVE flags on left-top corner (isActive should be pol independent)
        if PlotData_dict[polf]['isActive']:
            axf.text( -0.1, loc_y, "ACTIVE", transform=axf.transAxes,
                          horizontalalignment='left', verticalalignment='bottom', color='green', size=16,
                          style='italic', weight='bold' )
        else:
            axf.text( -0.1, loc_y, "INACTIVE", transform=axf.transAxes,
                          horizontalalignment='left', verticalalignment='bottom', color='red', size=16,
                          style='italic', weight='bold' )

        # if there are no data at all, draw a big 'NO DATA' to the first axes
        if pol0 == None:
            ax[pollist[0]].text( 0.5, 0.5, "NO DATA",
                                 transform=ax[pollist[0]].transAxes,
                                 ha='center', va='center', color='Gray', size=24,
                                 style='normal', weight='bold')
            for pol in pollist:
                ax[pol].axes.xaxis.set_visible(False)
                ax[pol].axes.yaxis.set_visible(False)

        return


    def __plot_data_to_axes( self, pollist:List[str], ax:MplAxes,
                             PlotData:Dict, data:Optional[Dict],
                             ScaleOut:Optional[List[float]], LowRange:Optional[bool] ):
        """
        Plot data to axes.

        Args:
            pollist     : List of pols
            ax          : axes to plot
            Plotdata    : PlotData to plot
            data        : data to plot
            ScaleOut    : scale out values  (optional for NO DATA case)
            LowRange    : Low Range flag    (optional for NO DATA case)
        Returns:
            (none)
        """
        # pol info goes to legend when overplotting multiple pols
        pp = pollist.index( PlotData['pol'] )
        legend_loc_y = 0.99 + 0.04 * ( len(pollist)-pp )
        if len(pollist)>1:
            polstr = "Pol: {}".format( PlotData['pol'] )
            ax.text( 0.00, legend_loc_y, polstr, transform=ax.transAxes, size=7, va='center' )

        # mark 'NO DATA' for NO DATA case
        if PlotData['data'] is None:
            ax.text( 0.07, legend_loc_y, "NO DATA", transform=ax.transAxes, size=7, va='center' )
            return

        # color and alpha defs (used for each pol)
        col = [
            { 'online': '0.5', 'normal': 'blue',  'deviator' : 'red',
              'thre': 'cyan',  'scaleout': 'red', 'gap0':'green', 'gap1':'cyan' },
            { 'online': '0.5', 'normal': 'blue',  'deviator' : 'red',
              'thre': 'cyan',  'scaleout': 'red', 'gap0':'green', 'gap1':'cyan' },
            { 'online': '0.5', 'normal': 'blue',  'deviator' : 'red',
              'thre': 'cyan',  'scaleout': 'red', 'gap0':'green', 'gap1':'cyan' },
            { 'online': '0.5', 'normal': 'blue',  'deviator' : 'red',
              'thre': 'cyan',  'scaleout': 'red', 'gap0':'green', 'gap1':'cyan' }
        ]
        alpha = [ 1.0, 0.4, 0.16, 0.064 ]

        # Regular Plot
        ax.plot( sd_display.mjd_to_plotval(data['online_x']), data['online_y'], 's',
                 markersize=1.7, color=col[pp]['online'], markeredgewidth=0,
                 alpha=alpha[pp], label='flagged (online)' )
        ax.plot( sd_display.mjd_to_plotval(data['normal_x']), data['normal_y'], 'o',
                 markersize=1.5, color=col[pp]['normal'], markeredgewidth=0,
                 alpha=alpha[pp], label='data below threshold' )
        ax.plot( sd_display.mjd_to_plotval(data['deviator_x']), data['deviator_y'], 'o',
                 markersize=2.5, color=col[pp]['deviator'], markeredgewidth=0,
                 alpha=alpha[pp], label='deviator' )
        ax.axhline( y=ScaleOut[0][0], linewidth=1, color=col[pp]['scaleout'],
                    label='vertical limit(s)', alpha=alpha[pp] )
        if PlotData['threType'] != "plot":
            ax.axhline( y=PlotData['thre'][0], linewidth=1, color=col[pp]['thre'],
                        alpha=alpha[pp], label=PlotData['threDesc'] )
            if LowRange:
                ax.axhline( y=PlotData['thre'][1], linewidth=1, color=col[pp]['thre'], alpha=alpha[pp] )
                ax.axhline( y=ScaleOut[1][0], linewidth=1, color=col[pp]['scaleout'], alpha=alpha[pp] )
        else:
            ax.plot( sd_display.mjd_to_plotval(PlotData['time']), PlotData['thre'][0], '-',
                     linewidth=1, color=col[pp]['thre'], alpha=alpha[pp], label=PlotData['threDesc'] )

        # plot gaps
        if len(PlotData['gap']) > 0:
            for row in sd_display.mjd_to_plotval(PlotData['gap'][0]):
                ax.axvline(x=row, linewidth=0.5, color=col[pp]['gap0'], ymin=0.95, alpha=alpha[pp])
        if len(PlotData['gap']) > 1:
            for row in sd_display.mjd_to_plotval(PlotData['gap'][1]):
                ax.axvline(x=row, linewidth=0.5, color=col[pp]['gap1'], ymin=0.9, ymax=0.95, alpha=alpha[pp])

        # legends
        ax.legend(loc='center left', numpoints=1, ncol=5,
                  prop={'size': 7}, frameon=False,
                  bbox_to_anchor=(0.06, legend_loc_y),
                  borderpad=0, handletextpad=0.5,
                  handlelength=1, columnspacing=1,
                  markerscale=2)
        return
