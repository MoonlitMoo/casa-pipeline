"""Template files generator for NRO data reduction.

This class is called by exportdata task of NRO, and generates thease files
into products folder:
- scale file: norscalefile.csv
- reduction template: rebase_and_image.py
"""

import glob
import itertools
import string
import os
from typing import Generator, List

from pipeline.domain.measurementset import MeasurementSet
from pipeline.domain.singledish import MSReductionGroupDesc, MSReductionGroupMember
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
import pipeline.hsd.tasks.common.observatory_policy as observatory_policy
from pipeline.infrastructure.launcher import Context

# the logger for this module
LOG = infrastructure.get_logger(__name__)


def generate_template(filename: str) -> string.Template:
    """Generate template using a specified file.

    Args:
        filename : template file name

    Returns:
        template text
    """
    with open(filename, 'r') as f:
        txt = f.read()

    template = string.Template(txt)
    return template


def export_template(filename: str, txt: str):
    """Save a text string to a file.

    Args:
        filename : file name to export
        txt : a text string to save
    """
    with open(filename, 'w') as f:
        f.write(txt)


def indent(level: int=0) -> str:
    """Generate indent string.

    Args:
        level : indent level. Defaults to 0.

    Returns:
        indent spaces
    """
    return '    ' * level


def space(n: int=0) -> str:
    """Generate space string.

    Args:
        n : space count. Defaults to 0.

    Returns:
        spaces
    """
    return ' ' * n


def get_template(name: str) -> str:
    """Get path of template file placed same place of this script.

    Args:
        name : template file name

    Returns:
        template path
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, name)


def generate_group_entries(ms: MeasurementSet,
                           member_list: List[MSReductionGroupMember])\
        -> Generator[str, None, None]:
    """Yield a CSV string for NRO scale file corresponding to ms and member_list.

    Args:
        ms : a MeasurementSet to filter member_list
        member_list : a list of MSReductionGroupMember

    Yields:
        Entries for CSV
    """
    # filter members by MS
    basename = ms.basename
    filtered_by_ms = filter(lambda x: x.ms.basename == basename, member_list)

    # generate list of antenna ID and spw ID pairs
    antenna_spw_pairs = set(((m.antenna.id, m.spw.id) for m in filtered_by_ms))

    # yield entries
    for antenna, spw in itertools.product(
            ms.antennas, ms.get_spectral_windows(science_windows_only=True)):
        if (antenna.id, spw.id) in antenna_spw_pairs:
            data_desc = ms.get_data_description(spw=spw.id)
            yield '\n'.join(
                # number of lines to be yielded is equal to number of correlations
                [','.join([basename, antenna.name, str(spw.id), corr, '1.0'])
                 for corr in data_desc.corr_axis]
            )


def generate_csv_entries(context: Context) -> Generator[str, None, None]:
    """Generate CSV entries of all MS and reduction groups in context.

    Args:
        context : pipeline context

    Yields:
        Entries for CSV
    """
    # reduction group
    reduction_group = context.observing_run.ms_reduction_group
    member_list = [m for k, v in reduction_group.items() for m in v]

    # measurement sets
    mses = context.observing_run.measurement_sets

    for ms in mses:
        for entry in generate_group_entries(ms, member_list):
            yield entry

    # finally EOF marker is yielded
    yield '#----------------------------------------------------------------------------------------------'


def generate_csv(context: Context, datafile: str) -> bool:
    """Generate CSV file using template file.

    Args:
        context : pipeline context
        datafile : CSV datafile generated

    Returns:
        whether CSV file was generated or not
    """
    tmp = get_template('scalefile.txt')
    with open(tmp, 'r') as f:
        txt = f.read()

    csv_entries = generate_csv_entries(context)
    txt = txt + '\n'.join(list(csv_entries))
    export_template(datafile, txt)

    return os.path.exists(datafile)


def generate_script(context: Context, scriptname: str, configname: str) -> bool:
    """Generate NRO data reduction template script file.

    Args:
        context : pipeline context
        scriptname : NRO data reduction template script: rebase_and_image.py
        configname : NRO data refuction configuration script:
                     rebase_and_image_config.py

    Returns:
        whether script was generated and configfile exists or not
    """
    tmp = get_template('template.txt')
    template = generate_template(tmp)

    myms = context.observing_run.measurement_sets[0]
    spws = [s.id for s in myms.get_spectral_windows(science_windows_only=True)]
    nspw = len(spws)

    # processing flags
    processspw = '\n'.join(
        [indent(level=3) + 'True' + indent(level=3) + '# SPW{}'.format(i)
         for i in spws])
    processspw = processspw.replace('True ', 'True,', nspw - 1)

    # baseline masks
    blrange = '\n'.join(
        [indent(level=5) + "''" + indent(level=2) + '# baseline_range for spw{}'.format(i)
         for i in spws])
    blrange = blrange.replace("'' ", "'',", nspw - 1)

    # rest frequencies
    images = glob.glob('*.spw*.cube.I.iter*.image.sd')
    rest_freqs = ['' for _ in spws]
    nchan = 0
    cell = []
    imsize = []
    phasecenter = ''
    qa = casa_tools.quanta
    for image in images:
        s = image.split('.')[-6]
        i = int(s.replace('spw', ''))
        if i in spws:
            index = spws.index(i)
            with casa_tools.ImageReader(image) as ia:
                coordsys = ia.coordsys()
                try:
                    imshape = ia.shape()
                    imsize = list(imshape[:2])
                    nchan = imshape[coordsys.findaxisbyname('Spectral')]
                    rest_freq = coordsys.restfrequency()
                    increments = coordsys.increment()['numeric']
                    units = coordsys.units()
                    _cell = [qa.convert(qa.quantity(abs(x[0]), x[1]), 'arcsec')
                             for x in (x for x in zip(increments[:2], units[:2]))]
                    cell = ['{value}{unit}'.format(**x) for x in _cell]
                    refcode = coordsys.referencecode()[0]
                    dummy = [0] * len(imshape)
                    dummy[0] = float(imshape[0]) / 2
                    dummy[1] = float(imshape[1]) / 2
                    world = coordsys.toworld(dummy, format='s')['string']
                    # Coordinate values may be in numerical value with unit
                    # instead of HMS/DMS format. Even so, leave the values as
                    # they are to keep precision
                    lon = world[0].replace(' ', '')
                    lat = world[1].replace(' ', '')
                    phasecenter = "'{} {} {}'".format(refcode, lon, lat)
                finally:
                    coordsys.done()

            if 'unit' in rest_freq and 'value' in rest_freq:
                rest_freqs[index] = '{}{}'.format(
                    rest_freq['value'][0], rest_freq['unit'])

    restfreqs = '\n'.join(
        [indent(level=5) + "'{}'".format(f) + indent(level=1) + '# Rest frequency of SPW{}'.format(i)
            for f, i in zip(rest_freqs, spws)])
    restfreqs = restfreqs.replace("' ", "',", nspw - 1)

    vis = myms.basename
    antennalist = [a.id for a in myms.antennas]
    source = myms.get_fields(intent='TARGET')[0].clean_name
    imaging_policy = observatory_policy.get_imaging_policy(context)
    convsupport = imaging_policy.get_convsupport()

    s = template.safe_substitute(processspw=processspw,
                                 baselinerange=blrange,
                                 restfreqs=restfreqs,
                                 nchan=nchan,
                                 cell=cell,
                                 phasecenter=phasecenter,
                                 imsize=imsize,
                                 convsupport=convsupport,
                                 vis=vis,
                                 antennalist=antennalist,
                                 source=source,
                                 config=os.path.basename(configname))

    export_template(scriptname, s)

    return os.path.exists(scriptname) and os.path.exists(configname)
