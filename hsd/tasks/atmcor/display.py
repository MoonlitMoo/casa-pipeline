"""Plotter for atmcor stage."""
import os
from typing import TYPE_CHECKING

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.renderer.logger as logger
from pipeline.domain.measurementset import MeasurementSet

if TYPE_CHECKING:
    from pipeline.infrastructure.jobrequest import JobRequest

LOG = infrastructure.get_logger(__name__)


class PlotmsRealVsFreqPlotter(object):
    """Plotter class to generate real_vs_freq plot."""

    def __init__(self,
                 ms: MeasurementSet,
                 atmvis: str,
                 atmtype: int,
                 datacolumn: str ='data',
                 output_dir: str ='.'):
        """Initialize plotter class.

        Args:
            ms: ms domain object
            atmvis: name of the MS produced by atmcor stage
            atmtype: ATM type
            datacolumn: datacolumn parameter. Defaults to 'data'.
            output_dir: output directory name. Defaults to '.'.
        """
        self.ms = ms
        self.atmvis = atmvis.rstrip('/')
        self.atmtype = atmtype
        self.spw = ''
        self.antenna = ''
        self.field = ''
        self.clean_field_name = ''
        self.datacolumn = datacolumn
        self.output_dir = output_dir

    @property
    def name(self) -> str:
        """Return full path to the MS."""
        return self.ms.name

    @property
    def basename(self) -> str:
        """Return basename of the MS."""
        return self.ms.basename

    @property
    def original_field_name(self) -> str:
        """Return field name without quoting."""
        return self.field.strip('"')

    def set_spw(self, spw: str =''):
        """Set spw selection.

        Args:
            spw: spw selecton. Defaults to ''.
        """
        self.spw = spw

    def set_antenna(self, antenna: str =''):
        """Set antenna selection.

        Args:
            antenna: antenna selection. Defaults to ''.
        """
        if isinstance(antenna, int):
            self.antenna = self.ms.antennas[antenna].name
        else:
            self.antenna = antenna

    def set_field(self, field: str =''):
        """Set field selection.

        Args:
            field: field selection. Defaults to ''.
        """
        if isinstance(field, int):
            self.field = self.ms.fields[field].name
            self.clean_field_name = self.ms.fields[field].clean_name
        else:
            self.field = field
            self.clean_field_name = field

    def get_antenna_selection(self) -> str:
        """Generate antenna selection string.

        Returns:
            antenna selection string.
        """
        if len(self.antenna) == 0:
            antenna = self.antenna
        else:
            antenna = '{}&&&'.format(self.antenna)
        return antenna

    def get_color_axis(self) -> str:
        """Get color axis depending on antenna selection.

        Returns:
            color axis value.
        """
        if len(self.antenna) == 0:
            coloraxis = 'antenna1'
        else:
            coloraxis = 'corr'
        return coloraxis

    def get_title(self) -> str:
        """Generate plot title.

        Returns:
            title string.
        """
        title = '\n'.join(
            [
                'ATM Corrected Real vs Frequency',
                '{} ATMType {} {} Spw {} Antenna {}'.format(
                    self.basename,
                    self.atmtype,
                    ('all' if self.field == '' else self.original_field_name),
                    ('all' if self.spw == '' else self.spw),
                    ('all' if self.antenna == '' else self.antenna),
                ),
                'Coloraxis {}'.format(self.get_color_axis())
            ]
        )
        return title

    def get_plotfile_name(self) -> str:
        """Generate plot file name (full path).

        Returns:
            full path to plot file name.
        """
        plotfile = '{}-atmtype_{}-{}-spw_{}-antenna_{}-atmcor-TARGET-real_vs_freq.png'.format(
            self.basename,
            self.atmtype,
            ('all' if self.field == '' else self.clean_field_name),
            ('all' if self.spw == '' else self.spw),
            ('all' if self.antenna == '' else self.antenna),
        )
        return os.path.join(self.output_dir, plotfile)

    def get_plot(self, task: 'JobRequest', plotfile: str) -> logger.Plot:
        """Generate Plot object.

        Args:
            task: JobRequest for plotms task call
            plotfile: name of the plot file

        Returns:
            Plot object.
        """
        parameters = {
            'vis': self.name,
            'ant': self.antenna,
            'spw': self.spw,
            'field': self.original_field_name,
        }
        plot = logger.Plot(
            plotfile,
            x_axis='Frequency',
            y_axis='Real',
            parameters=parameters,
            command=str(task)
        )
        return plot

    def plot(self) -> logger.Plot:
        """Execute plotms task to generate Real vs Freq plot.

        Returns:
            Plot object.
        """
        title = self.get_title()
        plotfile = self.get_plotfile_name()
        antenna = self.get_antenna_selection()
        coloraxis = self.get_color_axis()
        task_args = {
            'vis': self.atmvis,
            'xaxis': 'freq',
            'yaxis': 'real',
            'ydatacolumn': self.datacolumn,
            'spw': self.spw,
            'antenna': antenna,
            'field': self.field,
            'intent': 'OBSERVE_TARGET#ON_SOURCE',
            'coloraxis': coloraxis,
            'showgui': False,
            'title': title,
            'plotfile': plotfile,
            'showlegend': True,
            'averagedata': True,
            'avgtime': '1e8',
            'avgscan': True,
            'showatm': True,
        }
        task = casa_tasks.plotms(**task_args)
        if os.path.exists(plotfile):
            LOG.debug('Returning existing plot')
        else:
            task.execute()

        plot = self.get_plot(task, plotfile)
        return plot
