import collections
# import distutils.spawn as spawn
import itertools
import operator
# import platform
import xml.etree.cElementTree as eltree
from xml.dom import minidom

from pipeline import environment

# # Set the command used to calculate MD5 (see infrastructure.renderer.logger)
# CHECKSUM_CMD = None
#
# if platform.system() == 'Darwin':
#     # Look for md5 on OS X.
#     md5_path = spawn.find_executable('md5')
#     if md5_path:
#         LOG.trace('Using md5 executable at \'%s\' to generate MD5'
#                   % md5_path)
#         CHECKSUM_CMD = lambda name : (md5_path, name)
# else:
#     # .. otherwise try to find md5sum command.
#     md5sum_path = spawn.find_executable('md5sum')
#     if md5sum_path:
#         LOG.trace('Using convert executable at \'%s\' to generate MD5' % \
#                    md5sum_path)
#         CHECKSUM_CMD = lambda name : (md5sum_path, name)
#     else:
#         LOG.warning('Could not find command to calculate checksum. MD5 will not be written to manifest.')


class PipelineManifest(object):
    """
    Class for creating the pipeline data product manifest
    """
    def __init__(self, ouss_id):
        self.ouss_id = ouss_id
        self.piperesults = eltree.Element("piperesults", name=ouss_id)

    def import_xml(self, xmlfile):
        """
        Import the manifest from an existing manifest file
        """
        with open(xmlfile, 'r') as f:
            lines = [x.replace('\n', '').strip() for x in f.readlines()]
            self.piperesults = eltree.fromstring(''.join(lines))

    def set_ous(self, ous_name):
        """
        Set an OUS element and return it
        """
        return eltree.SubElement(self.piperesults, "ous", name=ous_name)

    def get_ous(self):
        """
        Currently this assumes there is only one ous as is the case
        for member ous processing
        """
        return self.piperesults.getchildren()[0]

    @staticmethod
    def add_manifest(ous, manifestname, ous_name="N/A", level="N/A", package="N/A"):
        """
        Set the manifest information
        """
        eltree.SubElement(ous, "manifest", name=manifestname, ous=ous_name, level=level, package=package, datatype="pipeline_manifest", format="xml")

    @staticmethod
    def add_casa_version(ous, casa_version):
        """
        Set the CASA version
        """
        eltree.SubElement(ous, "casaversion", name=casa_version)

    @staticmethod
    def get_casa_version(ous):
        """
        Get the CASA version
        """
        version = ous.find('casaversion')
        if version is not None:
            return version.attrib['name']
        return None

    @staticmethod
    def add_environment_info(ous):
        # group node information by host
        root = eltree.SubElement(ous, 'execution_environment')
        groups = []
        data = sorted(environment.cluster_details(), key=operator.attrgetter('hostname'))
        for _, g in itertools.groupby(data, operator.attrgetter('hostname')):
            groups.append(list(g))

        for host_details in groups:
            PipelineManifest.add_execution_node(root, host_details)

    @staticmethod
    def add_execution_node(root, host_details):
        mpi_server_details = [d for d in host_details if 'MPI Server' in d.role]
        num_mpi_servers = str(len(mpi_server_details))
        eltree.SubElement(root, 'node', num_mpi_servers=num_mpi_servers)

    @staticmethod
    def get_execution_nodes(ous):
        return ous.findall('./execution_environment/node')

    @staticmethod
    def add_pipeline_version(ous, pipeline_version):
        """
        Set the pipeline version
        """
        eltree.SubElement(ous, "pipeline_version", name=pipeline_version)

    @staticmethod
    def get_pipeline_version(ous):
        """
        Get the pipeline version
        """
        version = ous.find('pipeline_version')
        if version is not None:
            return version.attrib['name']
        return None

    @staticmethod
    def add_procedure_name(ous, procedure_name):
        """
        Set the procedure name
        """
        eltree.SubElement(ous, "procedure_name", name=procedure_name)

    @staticmethod
    def set_session(ous, session_name):
        """
        Set a SESSION element in an OUS element and return it
        """
        return eltree.SubElement(ous, "session", name=session_name)

    @staticmethod
    def get_session(ous, session_name):
        """
        Get a SESSION element in an OUS element and return it
        """
        for session in ous.iter('session'):
            if session.attrib['name'] == session_name:
                return session
        return None

    @staticmethod
    def get_asdm(session, asdm_name):
        """
        Get an ASDM element in a SESSION element and return it
        """
        for asdm in session.iter('asdm'):
            if asdm.attrib['name'] == asdm_name:
                return asdm
        return None

    @staticmethod
    def add_caltables(session, caltables_file, session_name, ous_name="N/A", level="N/A", package="N/A"):
        eltree.SubElement(session, "caltables", name=caltables_file, ous_name=ous_name, level=level, session=session_name, package=package, datatype="caltables", format="tgz")

    @staticmethod
    def add_auxcaltables(session, caltables_file, session_name, ous_name="N/A", level="N/A", package="N/A"):
        eltree.SubElement(session, "aux_caltables", name=caltables_file, level=level, session=session_name, package=package, datatype="caltables", format="tgz")

    @staticmethod
    def get_caltables(ous):
        caltables_dict = collections.OrderedDict()
        for session in ous.iter('session'):
            for caltable in session.iter('caltables'):
                caltables_dict[session.attrib['name']] = caltable.attrib['name']
        return caltables_dict

    @staticmethod
    def add_asdm(session, asdm_name, flags_file, calapply_file):
        """
        Add an ASDM element to a SESSION element
        """
        asdm = eltree.SubElement(session, "asdm", name=asdm_name, datatype='asdm', format='sdm')
        eltree.SubElement(asdm, "finalflags", name=flags_file, asdm=asdm_name, datatype="ms.flagversions", format="tgz")
        eltree.SubElement(asdm, "applycmds", name=calapply_file)

    @staticmethod
    def add_asdm_imlist(session, asdm_name, ms_file, flags_file, calapply_file, imagelist, imtype):
        """
        Add an ASDM element to a SESSION element
        """
        asdm = eltree.SubElement(session, "asdm", name=asdm_name, datatype="asdm", format="sdm")
        if ms_file is not None:
            eltree.SubElement(asdm, "finalms", name=ms_file)
        if flags_file is not None:
            eltree.SubElement(asdm, "finalflags", name=flags_file, asdm=asdm_name, datatype="ms.flagversions", format="tgz")
        if calapply_file is not None:
            eltree.SubElement(asdm, "applycmds", name=calapply_file, asdm=asdm_name, datatype="ms.calapply", format="txt")
        for image in imagelist:
            eltree.SubElement(asdm, "image", name=image, imtype=imtype)

    @staticmethod
    def add_auxasdm(session, asdm_name, calapply_file):
        """
        Add an ASDM element to a SESSION element
        """
        asdm = eltree.SubElement(session, "aux_asdm", name=asdm_name, datatype="asdm", format="sdm")
        eltree.SubElement(asdm, "applycmds", name=calapply_file)

    @staticmethod
    def get_final_flagversions(ous):
        """
        Get a list of the final flag versions
        """
        finalflags_dict = collections.OrderedDict()
        for session in ous.iter('session'):
            for asdm in session.iter('asdm'):
                for finalflags in asdm.iter('finalflags'):
                    finalflags_dict[asdm.attrib['name']] = finalflags.attrib['name']
        return finalflags_dict

    @staticmethod
    def get_applycals(ous):
        """
        Get a list of the final applycal instructions
        """
        applycmds_dict = collections.OrderedDict()
        for session in ous.iter('session'):
            for asdm in session.iter('asdm'):
                for applycmds in asdm.iter('applycmds'):
                    applycmds_dict[asdm.attrib['name']] = applycmds.attrib['name']
        return applycmds_dict

    @staticmethod
    def add_pprfile(ous, ppr_file, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the pipeline processing request file to the OUS element
        """
        eltree.SubElement(ous, "piperequest", name=ppr_file, level=level, ous=ous_name, package=package, datatype="pprequest", format="xml")

    @staticmethod
    def add_images(ous, imagelist, imtype, extra_attributes_list=None):
        """
        Add a list of images to the OUS element. Note that this does not have
        to be an ous element, e.d. an asdm element will do
        """
        for i, image in enumerate(imagelist):
            # "manualstring" is a special attribute requested in PIPE-1105 to
            # distinguish pipeline products from manually reduced ones. For
            # pipeline runs "manualstring" is always "N/A".
            if extra_attributes_list is None:
                eltree.SubElement(ous, "image", name=image, imtype=imtype, manualstring="N/A")
            else:
                eltree.SubElement(ous, "image", name=image, imtype=imtype, manualstring="N/A", **extra_attributes_list[i])

    @staticmethod
    def add_pipescript(ous, pipescript, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the pipeline processing script to the OUS element
        """
        eltree.SubElement(ous, "pipescript", name=pipescript, level=level, ous=ous_name, package=package, datatype="casa_pipescript", format="py")

    @staticmethod
    def add_restorescript(ous, restorescript, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the pipeline restore script to the OUS element
        """
        eltree.SubElement(ous, "restorescript", name=restorescript, level=level, ous=ous_name, package=package, datatype="casa_piperestorescript", format="py")

    @staticmethod
    def add_weblog(ous, weblog, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the weblog to the OUS element
        """
        eltree.SubElement(ous, "weblog", name=weblog, level=level, ous=ous_name, package=package, datatype="weblog", format="tgz")

    @staticmethod
    def add_casa_cmdlog(ous, casa_cmdlog, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the CASA commands log to the OUS element
        """
        eltree.SubElement(ous, "casa_cmdlog", name=casa_cmdlog, level=level, ous=ous_name, package=package, datatype="casa_commands", format="log")

    @staticmethod
    def add_flux_file(ous, flux_file):
        """
        Add the flux file to the OUS element
        Remove at some point.
        """
        eltree.SubElement(ous, "flux_file", name=flux_file)

    @staticmethod
    def add_antennapos_file(ous, antennapos_file):
        """
        Add the antenna positions file to the OUS element
        Remove at some point
        """
        eltree.SubElement(ous, "antennapos_file", name=antennapos_file)

    @staticmethod
    def add_cont_file(ous, cont_file):
        """
        Add the continuum frequency ranges file to the OUS element
        Remove at some point
        """
        eltree.SubElement(ous, "cont_file", name=cont_file)

    @staticmethod
    def add_aux_products_file(ous, auxproducts_file, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the auxiliary products file. Is one enough ?
        """
        eltree.SubElement(ous, "aux_products_file", name=auxproducts_file, level=level, ous=ous_name, package=package, datatype="auxproducts", format="tgz")

    @staticmethod
    def add_aqua_report(ous, aqua_report, ous_name="N/A", level="N/A", package="N/A"):
        """
        Add the AQUA report to the OUS element
        """
        eltree.SubElement(ous, "aqua_report", name=aqua_report, level=level, ous=ous_name, package=package, datatype="pipeline_aquareport", format="xml")

    def add_renorm(self, asdm_name, inputs):
        """
        Add the renormalization parameters to a asdm element
        """
        for asdm in self.get_ous().findall(f".//asdm[@name=\'{asdm_name}\']"):
            newinputs = {key:str(value) for (key,value) in inputs.items()} # stringify the values
            eltree.SubElement(asdm, "hifa_renorm", newinputs)

    def get_renorm(self, asdm_name):
        """
        Get the hifa_renorm element
        """
        for asdm in self.get_ous().findall(f".//asdm[@name=\'{asdm_name}\']"):
            return getattr(asdm.find('hifa_renorm'), 'attrib', None)
        else:
            return None

    def write(self, filename):
        """
        Convert the document to a nicely formatted XML string
        and save it in a file
        """
        xmlstr = eltree.tostring(self.piperesults, 'utf-8')

        # Reformat it to prettyprint style
        reparsed = minidom.parseString(xmlstr)
        reparsed_xmlstr = reparsed.toprettyxml(indent="  ")

        # Save it to a file.
        with open(filename, "w") as manifestfile:
            manifestfile.write(reparsed_xmlstr)
