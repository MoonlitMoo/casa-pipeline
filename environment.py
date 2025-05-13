"""
environment.py defines functions and variables related to the execution environment.
"""
from __future__ import annotations

import ast
import dataclasses
import json
import operator
import os
import platform
import re
import resource
import sys
import typing
from importlib.metadata import version, PackageNotFoundError
from importlib.util import find_spec
from pathlib import Path

import casatasks
import pkg_resources

from .infrastructure import casa_tools
from .infrastructure import logging
from .infrastructure import mpihelpers
from .infrastructure import utils
from .infrastructure.mpihelpers import MPIEnvironment
from .infrastructure.utils.subprocess import safe_run
from .infrastructure.version import get_version

LOG = logging.get_logger(__name__)

__all__ = ['casa_version', 'casa_version_string', 'compare_casa_version', 'pipeline_revision', 'cluster_details',
           'dependency_details']


def _load(path: str, encoding: str = 'UTF-8') -> typing.AnyStr:
    """
    Read in a file and return the contents

    @param path: path to file
    @param encoding: optional encoding used to decode the file
    """
    with open(path, 'r', encoding=encoding, newline="") as file:
        return file.read()


class Environment(typing.Protocol):
    """
    Environment is a Protocol that collects the attributes that describe a
    pipeline execution environment.

    This Protocol is intended to help with type safety, ensuring that
    implementations for MacOS and Linux provide all required data in the
    correct type.

    There are several bits of information collected on CPU which are
    subtly different:

    - logical_cpu_cores and physical_cpu_cores reports how many CPUs are
      present on the machine. With SMT, logical CPU cores can be more than
      the number of physical cores.
    - casa_cores and casa_threads reports how many physical and logical cores
      are accessible to the running CASA process, as detected by CASA itself.

    On a Linux machine, attributes describing the cgroup limits applied to the
    CASA process are also populated:

    - cgroup_cpus reports how many CPU cores have been allocated to the
      running process, as detected through cgroup limits.
    - cgroup_bandwidth reports the CPU bandwidth limits applied by cgroups.
      'bandwidth' means the fraction of CPU time for CPUs allocated to the
      cgroup, e.g., 25% of cores 1-4
    - casa_cores and cgroup_cpus should generally (always?) be equal, but I
      don't know enough about the CASA implementation to guarantee that.

    The most complex scenario this can describe is, for example, that CASA has
    been allocated 25% of cores 1-4 on a machine with 10 physical cores and
    14 logical cores. There are other cgroup attributes that could affect the
    amount of time CASA receives (e.g., CPU weighting), but those are ignored
    for now. We also do not attempt to detect the difference between p-cores
    and e-cores, though that may be a factor for non-cluster setups.
    """

    host_distribution: str      # OS description, e.g., RedHat Linux 8.3
    hostname: str               # hostname, e.g., somepc.nrao.edu

    cpu_type: str               # CPU description, e.g.,
    logical_cpu_cores: str      # logical core count
    physical_cpu_cores: str     # physical CPU count
    ram: int                    # ram in KB
    swap: str                   # swap size in bytes

    casa_cores: int             # Number of cores seen by CASA
    casa_threads: int           # Number of CASA threads
    casa_memory: int            # tclean-reported available memory, in bytes

    cgroup_num_cpus: str        # cgroup CPU allocation
    cgroup_cpu_bandwidth: str   # cgroup CPU bandwidth limit
    cgroup_cpu_weight: str      # cgroup CPU weight
    cgroup_mem_limit: str       # cgroup memory limit

    ulimit_files: str           # open file descriptors ulimit
    ulimit_cpu: str             # cpu time ulimit, in seconds
    ulimit_mem: str             # memory ulimit

    role: str                   # MPI role


class EnvironmentFactory:
    """
    EnvironmentFactory returns the Environment appropriate to the host.
    """
    @staticmethod
    def create() -> Environment:
        system = platform.system()
        if system == 'Linux':
            return LinuxEnvironment()
        elif system == 'Darwin':
            return MacOSEnvironment()
        else:
            raise NotImplemented('Could not query environment for system {!s}'.format(system))


class CommonEnvironment:
    """
    CommonEnvironment is a base class for environment properties that can be
    measured in an OS-independent fashion.

    CommonEnvironment does not provide all required properties to satisfy the
    Environment interface, and is not intended to be instantiated directly.
    """
    def __init__(self):
        logsink = casa_tools.logsink

        # number of physical cores as seen by CASA
        self.casa_cores = logsink.getNumCPUs(use_aipsrc=True)

        # number of threads (=logical cores) as seen by CASA
        self.casa_threads = logsink.ompGetNumThreads()

        # tclean-reported available memory [result in KB]
        self.casa_memory = logsink.getMemoryTotal(use_aipsrc=True) * 1024

        # hostname
        self.hostname = platform.node()

        # helper function to get limits set by ulimit
        def get_ulimit(limit_types):
            active_limits = {
                val
                for limit in limit_types
                for val in resource.getrlimit(limit)
                # ulimit on MacOS returns maximum 64-bit signed integer for unlimited
                if -1 < val < 9223372036854775807
            }
            return min(active_limits, default='N/A')

        #
        # The 'too many open files' error during imaging is often due to an
        # incorrect ulimit. The ulimit value is recorded per host to assist in
        # diagnosing these errors.
        #
        # See: PIPE-350; PIPE-338
        self.ulimit_files = get_ulimit([resource.RLIMIT_NOFILE])
        self.ulimit_cpu = get_ulimit([resource.RLIMIT_CPU])
        # three applicable limits for memory: RSS, heap size, and data seg size.
        self.ulimit_mem = get_ulimit([resource.RLIMIT_AS, resource.RLIMIT_RSS, resource.RLIMIT_DATA])

        if not MPIEnvironment.is_mpi_enabled:
            role = 'Non-MPI Host'
        elif MPIEnvironment.is_mpi_client:
            role = 'MPI Client'
        else:
            role = 'MPI Server'
        self.role = role


class LinuxEnvironment(CommonEnvironment):
    """
    LinuxEnvironment adds the Environment properties missing from a
    CommonEnvironment using methods that are specific to Linux.

    LinuxEnvironment depends on the command line utilities lscpu and swapon
    for its analysis of CPU and memory.

    LinuxEnvironment is expected to be instantiated.
    """

    def __init__(self):
        super(LinuxEnvironment, self).__init__()

        lscpu_json = json.loads(safe_run('lscpu -J', on_error='{"lscpu": {}}'))
        self.cpu_type = self._from_lscpu(lscpu_json, 'Model name:')
        self.logical_cpu_cores = self._from_lscpu(lscpu_json, 'CPU(s):')
        self.physical_cpu_cores = self._from_lscpu(lscpu_json, "Core(s) per socket:")

        self.ram = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        self.swap = safe_run('swapon --show=SIZE --bytes --noheadings')

        cgroup_controllers = CGroupControllerParser.get_controllers()
        self.cgroup_num_cpus = str(CGroupLimit.CPUAllocation.get_limit(cgroup_controllers))
        self.cgroup_cpu_bandwidth = str(CGroupLimit.CPUBandwidth.get_limit(cgroup_controllers))
        self.cgroup_cpu_weight = str(CGroupLimit.CPUWeight.get_limit(cgroup_controllers))
        self.cgroup_mem_limit = str(CGroupLimit.MemoryLimit.get_limit(cgroup_controllers))

        os_release = dict(self.read_os_release())
        self.host_distribution = (f'{os_release.get("NAME", "Linux")} '
                                  f'{os_release.get("VERSION", "(unknown distribution)")}')

    @staticmethod
    def read_os_release():
        """
        Reads OS release information from disk.

        Taken from https://www.freedesktop.org/software/systemd/man/latest/os-release.html
        """
        # in Python >= 3.10 we could do this:
        # return platform.freedesktop_os_release()
        try:
            filename = '/etc/os-release'
            f = open(filename)
        except FileNotFoundError:
            filename = '/usr/lib/os-release'
            f = open(filename)

        for line_number, line in enumerate(f, start=1):
            line = line.rstrip()
            if not line or line.startswith('#'):
                continue
            m = re.match(r'([A-Z][A-Z_0-9]+)=(.*)', line)
            if m:
                name, val = m.groups()
                if val and val[0] in '"\'':
                    val = ast.literal_eval(val)
                yield name, val
            else:
                LOG.warn(f'{filename}:{line_number}: bad line {line!r}')

    @staticmethod
    def _from_lscpu(lscpu_json: dict, key: str) -> str:
        """
        Extract data for the requested field from lscpu JSON output. If the field
        is not present, 'unknown' is returned.

        @param lscpu_json: dict of lscpu output, as produced by 'lscpu -J'
        @param key: name of field to extract
        """
        flattened = {d['field']: d['data'] for d in lscpu_json['lscpu']}
        vals = [v for k, v in flattened.items() if key == k]
        if len(vals) != 1:
            return 'unknown'
        return vals[0]


@dataclasses.dataclass
class CGroupController:
    """
    CgroupSpec is a dataclass for properties that describe a cgroup limit. It
    holds information on the path to the cgroup controller, cgroup type, and
    cgroup limit path itself.

    - name: name of the cgroup controller
    - hierarchy_id: cgroup hierarchy ID; 1 for cgroup v1, 0 for disabled or cgroup
      v2
    - enabled: indicates cgroup controller status, either 1 for enabled or 0 for
      disabled
    - cgroup_path: path to cgroup limit
    - mount_path: path to cgroup filesystem
    - root_mount_point: path to root mount point of cgroup filesystem
    """
    name: str
    hierarchy_id: int
    enabled: bool
    cgroup_path: Path | None  # optional as path not present when controller is disabled
    mount_path: Path | None  # optional as path not present when controller is disabled
    root_mount_path: Path | None  # optional as path not present when controller is disabled

    def get_limits(
            self,
            v1_attrs: typing.List[str],
            v2_attrs: typing.List[str]
    ) -> typing.List:
        """
        Get the cgroup limits for a list of cgroup attributes.

        @param v1_attrs: list of cgroup v1 attributes
        @param v1_attrs: list of cgroup v2 attributes
        """
        if not self.enabled:
            return []

        root_path = self.root_mount_path / self.mount_path
        cgroup_path = root_path / self.cgroup_path

        cgroups_to_consider = {cgroup_path, *cgroup_path.parents} - set(root_path.parents)
        limit_files = v2_attrs if self.hierarchy_id == 0 else v1_attrs

        limits = []
        for path in cgroups_to_consider:
            for f in limit_files:
                file_name = path / f
                if file_name.is_file():
                    limit = _load(file_name).strip()
                    if limit:
                        limits.append(limit)

        return limits


class CGroupControllerParser:

    @staticmethod
    def get_controllers() -> typing.Dict[str, CGroupController]:
        """
        Get a dict of cgroup controllers and mount points that can be used
        for parsing cgroup limits.

        Identifying the cgroup limits applicable to the running process
        requires parsing three files:

          - /proc/cgroups to determine cgroup controllers on the system
          - /proc/self/cgroup to determine cgroup controllers applicable to
            the running process
          - /proc/self/mountinfo to list the filesystem mounts - inclduding
            the cgroup virtual filesystem mount - for the running process

        Translated from
        https://github.com/openjdk/jdk/blob/jdk-17+18/src/hotspot/os/linux/cgroupSubsystem_linux.cpp

        @return: dict of CGroupControllers indexed by controller name.
        """
        results = {}

        # stage 1: read /proc/cgroups to list the available cgroup controllers
        # and their status
        try:
            controller_info = _load('/proc/cgroups')
        except (OSError, IOError):
            return {}

        for cgroup_spec in controller_info.splitlines():
            m = re.fullmatch(r"(?P<name>\w+)\s(?P<hierarchy_id>\d+)\s\d+\s(?P<enabled>\d+)", cgroup_spec.strip())
            if m is None:
                continue
            name = m.group("name")
            hierarchy_id = int(m.group("hierarchy_id"))
            enabled = int(m.group("enabled")) == 1
            controller = CGroupController(
                name=name,
                hierarchy_id=hierarchy_id,
                enabled=enabled,
                cgroup_path=None,
                mount_path=None,
                root_mount_path=None
            )
            results[name] = controller

        is_cgroups_v2 = safe_run('stat -fc %T /sys/fs/cgroup/') == 'cgroup2fs'

        # stage 2: read /proc/self/cgroup and determine:
        #   - the cgroup path for cgroups v2 or
        #   - on a cgroups v1 system, collect info for mapping
        #     the host mount point to the local one via /proc/self/mountinfo below.
        try:
            self_cgroup = _load('/proc/self/cgroup')
        except (OSError, IOError):
            return results

        for cgroup_spec in self_cgroup.splitlines():
            m = re.fullmatch(
                r"(?P<hierarchy_id>\d+):(?P<controllers>[^:]*):(?P<cgroup_path>.+)",
                cgroup_spec.strip(),
            )
            if m is None:
                continue
            cgroup_path = Path(m.group("cgroup_path")[1:])
            if is_cgroups_v2:
                for controller in results.values():
                    controller.cgroup_path = cgroup_path
            else:
                for controller in m.group("controllers").split(","):
                    if controller in results:
                        results[controller].cgroup_path = cgroup_path

        # stage 3: find mount points by reading /proc/self/mountinfo
        try:
            mountinfo = _load("/proc/self/mountinfo")
        except (OSError, IOError):
            return results

        for mount_spec in mountinfo.splitlines():
            m = re.fullmatch(
                r"\d+\s\d+\s\d+:\d+\s(?P<root>[^\s]+)\s(?P<mount_point>[^\s]+)\s[^-]+-\s(?P<fs_type>[^\s]+)\s[^\s]+\s(?P<cgroups>[^\s]+)",
                mount_spec.strip(),
            )
            if m is None:
                continue
            root = Path(m.group("root"))
            mount_point = Path(m.group("mount_point")[1:])
            fs_type = m.group("fs_type")
            if is_cgroups_v2 and fs_type == "cgroup2":
                for controller in results.values():
                    controller.mount_path = mount_point
                    controller.root_mount_path = root
            elif fs_type == "cgroup":
                # matched token will be similar to 'rw,memory'
                cgroups = m.group("cgroups").split(",")
                for token in cgroups:
                    if token in results:
                        results[token].mount_path = mount_point
                        results[token].root_mount_path = root

        return results


class CGroupLimit:

    class CPUWeight:
        """
        Distribution of CPU time to allocate for this process relative to
        other processes in the cgroup.

        The weights of the child cgroups that have running processes are
        summed up at the level of the parent cgroup. The CPU resource is then
        distributed proportionally based on the respective weights. As a
        result, when all processes run at the same time, the kernel allocates
        to each of them the proportionate CPU time based on their respective
        cgroup’s cpu.weight file.
        """

        def __init__(self, val: str):
            self.weight = int(val)

        def __str__(self):
            return f'{self.weight}%'

        @staticmethod
        def get_limit(controllers: typing.Dict[str, CGroupController]):
            controller = controllers['cpu']
            str_limits = controller.get_limits([], ['cpu.weight'])
            limits = [CGroupLimit.CPUWeight(val) for val in str_limits]
            return min(limits, key=operator.attrgetter('weight'), default='N/A')

    class CPUBandwidth:
        """
        CPUBandwidth describes a cgroup CPU bandwidth quota, the fraction of
        time that the collective processes in a cgroup can use the CPUs
        allocated to the cgroup.

        A cgroup CPU Bandwidth is composed of two parts:

        - quota: the allowed time quota in microseconds for which all processes
          collectively in a child group can run during one period, or 'max' if
          no limit is applied
        - period: length of the period.

        During a single period, when processes in a control group collectively
        exhaust the time specified by this quota, they are throttled for the
        remainder of the period and not allowed to run until the next period.

        For example, a quota of 25000 and period of 100000 means that,
        collectively, the processes in this cgroup can collectively use 0.025s
        of CPU time every 0.1s.
        """

        def __init__(self, val: str):
            quota, period = val.split(' ')
            if quota == 'max' or quota == '-1':
                quota = period

            self.quota = int(quota)
            self.period = int(period)
            self.ratio = self.quota / self.period

        def __str__(self):
            return f'{self.ratio:.0%}'

        @staticmethod
        def get_limit(controllers: typing.Dict[str, CGroupController]):
            controller = controllers['cpu']

            if controller.enabled and controller.hierarchy_id == 0:
                str_limits = controller.get_limits([], ['cpu.max'])
            else:
                # cgroup v1 CPU quota definitions are split across two parameters
                periods = controller.get_limits(['cpu.cfs_period_us'], [])
                quotas = controller.get_limits(['cpu.cfs_quota_us'], [])
                str_limits = [f'{q} {p}' for (q, p) in zip(quotas, periods)]

            limits = [CGroupLimit.CPUBandwidth(val) for val in str_limits]
            return min(limits, key=operator.attrgetter('ratio'), default='N/A')

    class CPUAllocation:
        """
        CPUAllocation represents a cgroup CPU allocation.

        A cgroup CPU allocation is described as a string. For example, a
        cgroup value of '1-3,5,7-9' would mean CPU cores 1,2,3,5,7,8, and 9
        can be used by processes in the cgroup.
        """

        def __init__(self, val: str):
            self.cpus = set(self._expand(val))
            self.num_cpus = len(self.cpus)

        @staticmethod
        def _expand(s: str) -> typing.List[int]:
            """
            Converts a CPU allocation from the original cgroups format to an
            equivalent list of integer CPU IDs.

            Example: converts '1-4,7,8' to [1,2,3,4,7,8]
            """
            r = []
            for i in s.split(','):
                if '-' not in i:
                    r.append(int(i))
                else:
                    l, h = map(int, i.split('-'))
                    r += range(l, h + 1)
            return r

        def __str__(self):
            return f'{self.num_cpus}'

        @staticmethod
        def get_limit(controllers: typing.Dict[str, CGroupController]) -> CGroupLimit.CPUAllocation:
            controller = controllers['cpuset']
            str_limits = controller.get_limits(
                ['cpuset.cpus'],
                ['cpuset.cpus', 'cpuset.cpus.effective']
            )
            limits = [CGroupLimit.CPUAllocation(val) for val in str_limits]
            return min(limits, key=operator.attrgetter('num_cpus'), default='N/A')

    class MemoryLimit:
        """
        Represents the limiting cgroup limit on RAM+swap usage for a process.
        """

        # No cgroup memory limits can be reported as max or as a magic number
        #
        # See https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/pull-requests/1244/overview?commentId=13998
        # and links therein
        _UNLIMITED = ('max', str(sys.maxsize - (sys.maxsize % os.sysconf('SC_PAGE_SIZE'))))

        def __init__(self, val: str):
            if val in CGroupLimit.MemoryLimit._UNLIMITED:
                self.limit = -1
            else:
                self.limit = int(val)

        def __str__(self):
            return f'{self.limit}'

        @staticmethod
        def get_limit(controllers: typing.Dict[str, CGroupController]):
            controller = controllers['memory']
            str_limits = controller.get_limits(
                ["memory.limit_in_bytes", "memory.memsw.limit_in_bytes", "memory.soft_limit_in_bytes"],
                ["memory.high", "memory.max"]
            )
            # -1 = remove any existing limits
            limits = [CGroupLimit.MemoryLimit(val) for val in str_limits]
            try:
                limit = min([l for l in limits if l.limit >= 0], key=operator.attrgetter('limit'))
            except ValueError:
                # empty list = no active memory limits
                return 'N/A'

            return limit.limit


class MacOSEnvironment(CommonEnvironment):
    """
    MacOSEnvironment adds environment properties missing from a CommonEnvironment
    using methods that are specific to MacOS.

    Unlike CommonEnvironment, MacOSEnvironment is expected to be instantiated.
    """

    def __init__(self):
        super(MacOSEnvironment, self).__init__()

        self.cpu_type = self._from_sysctl('machdep.cpu.brand_string')
        self.logical_cpu_cores = self._from_sysctl('hw.logicalcpu')
        self.physical_cpu_cores = self._from_sysctl('hw.physicalcpu')
        self.host_distribution = 'MacOS {!s}'.format(platform.mac_ver()[0])

        self.ram = int(self._from_sysctl('hw.memsize'))
        self.swap = self._get_swap()

        self.cgroup_num_cpus = 'N/A'
        self.cgroup_cpu_bandwidth = 'N/A'
        self.cgroup_cpu_weight = 'N/A'
        self.cgroup_mem_limit = 'N/A'

    @staticmethod
    def _from_sysctl(prop: str) -> str:
        return safe_run(f'sysctl -n {prop}')

    @staticmethod
    def _get_swap():
        """
        Extract total swap usage from a line like:

        vm.swapusage: total = 1024.00M  used = 191.00M  free = 833.00M  (encrypted)
        """
        swap = MacOSEnvironment._from_sysctl('vm.swapusage')
        m = re.search(r"\S+ total = (?P<total>\S+)", swap)
        if m is None:
            return 'unknown'
        return m.group("total")


# Determine pipeline version from Git or package.
def _pipeline_revision() -> str:
    """
    Get a string describing the pipeline revision and branch of the executing
    pipeline distribution if executing from a Git repo; as a fall-back,
    attempt to get version from built package.
    :return: string describing pipeline version.
    """
    pl_path = pkg_resources.resource_filename(__name__, '')
    return get_version(pl_path)


casa_version = casatasks.version()
casa_version_string = casatasks.version_string()
compare_casa_version = casa_tools.utils.compare_version


def _get_dependency_details(package_list=None):
    """Get dependency package version/path.

    See https://docs.python.org/3.8/library/importlib.metadata.html#metadata
    """
    if package_list is None:
        package_list = ['numpy', 'scipy', 'matplotlib', 'astropy', 'bdsf',
                        'casatools', 'casatasks', 'almatasks', 'casadata',
                        'casampi', 'casaplotms']

    package_details = dict.fromkeys(package_list)
    for package in package_list:
        try:
            package_version = version(package)
            module_spec = find_spec(package)
            if module_spec is not None:
                package_details[package] = {
                    'version': package_version,
                    'path': os.path.dirname(module_spec.origin)
                }
        except PackageNotFoundError:
            pass
    return package_details


dependency_details = _get_dependency_details()
iers_info = utils.IERSInfo()
pipeline_revision = _pipeline_revision()

ENVIRONMENT = EnvironmentFactory.create()

def cluster_details():
    # defer calculation as running this code at import time blocks MPI
    # see https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/pull-requests/1244/overview?commentId=13655
    global _cluster_details
    if _cluster_details is None:
        env_details = [ENVIRONMENT]
        if mpihelpers.is_mpi_ready():
            mpi_results = mpihelpers.mpiclient.push_command_request(
                'pipeline.environment.ENVIRONMENT',
                block=True,
                target_server=mpihelpers.mpi_server_list
            )
            for r in mpi_results:
                env_details.append(r['ret'])

        _cluster_details = env_details

    return _cluster_details

_cluster_details = None
