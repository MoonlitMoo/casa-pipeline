# This script has to be run within the Git repository.
# It will inspect the state of the current tree/branch in the workspace.

# Version type is used to select the appropriate grep pattern.
# The idea is to look for either the latest master/release type tag "\d+.\d+.\d+.\d+"
# or PIPE-<number>-x tags. Both are required to construct a meaningful number for
# the pipeline wheels.
import dataclasses
import functools
import re
import subprocess
import sys
from io import StringIO
from typing import Optional

try:
    from logging import getLogger
except (ModuleNotFoundError, AttributeError):
    # * ModuleNotFoundError = when running setup.py without CASA libs
    #   installed, e.g., a pristine venv
    # * AttributeError = CASA libs present but pipeline.infrastructure.logging
    #   module cannot be imported, resulting in this error:
    #
    #   AttributeError: partially initialized module 'logging' has no attribute
    #   'getLogger' (most likely due to a circular import)
    #
    # For both cases, which only occur when run through setup.py, detect the
    # failure and fake the logger functionality required by this module.
    def getLogger(name):
        class FakeLogger:
            def exception(self, msg, *args, **kwargs):
                print('Exception: {msg}')

        return FakeLogger()

LOG = getLogger(__name__)


def _run(command: str, stdout=None, stderr=None, cwd=None, shell=True) -> int:
    """
    Run a command in a subprocess.

    This helper function is intended to hide the boilerplate required to
    create and handle a subprocess while capturing its output. Rather than
    having functions call subprocess directly, they should consider calling
    this routine so that we have uniform handling.

    @param command: the command to execute
    @param stdout: optional stream to direct stdout to
    @param stderr: optional stream to direct stderr to
    @param shell:
    @param cwd: working directory for command
    @return: exit code of the process in which the command executed
    """
    stdout = stdout or sys.stderr
    stderr = stderr or sys.stderr

    out = subprocess.PIPE if isinstance(stdout, StringIO) else stdout
    err = subprocess.PIPE if isinstance(stderr, StringIO) else stderr

    proc = subprocess.Popen(command, shell=shell, stdout=out, stderr=err, cwd=cwd)

    proc_stdout, proc_stderr = proc.communicate()
    if proc_stdout:
        stdout.write(proc_stdout.decode("utf-8", errors="ignore"))
    if proc_stderr:
        stderr.write(proc_stderr.decode("utf-8", errors="ignore"))
    return proc.returncode


def _safe_run(command: str, on_error: str = 'N/A', cwd: Optional[str] = None, log_errors=True) -> str:
    """
    Safely run a command in a subprocess, returning the given string if an
    error occurs.

    @param command: the command to execute
    @param on_error: message to return if an exception occurs
    @param cwd: working directory for command
    @param log_errors: whether to log errors that occur while running the command
    @return: process output or error message
    """
    stdout = StringIO()
    try:
        exit_code = _run(command, stdout=stdout, stderr=subprocess.DEVNULL, cwd=cwd)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        if log_errors:
            LOG.exception(f'Error running {command}', exc_info=e)
    else:
        if exit_code == 0:
            return stdout.getvalue().strip()

    return on_error


@dataclasses.dataclass
class VersionInfo:
    """
    VersionInfo holds information about the pipeline version, as populated by
    analysis of the git repo, or for built Python wheels, from reading of a
    static version file.
    """

    commit_hash: str
    branch: str
    branch_tag: str
    release_tag: str
    dirty: bool

    @property
    def pipeline_version(self) -> str:
        commit_hash = 'unknown_hash' if self.commit_hash == 'N/A' else self.commit_hash
        branch = 'unknown_branch' if self.branch in ('N/A', 'HEAD') else self.branch

        if branch == "main" or branch.startswith("release/"):
            local_version_label = '+dirty' if self.dirty else ''
        else:
            local_version_label = '+' + self._sanitise(f'{commit_hash}-{branch}')

        return f"{self.release_tag}{local_version_label}"

    @staticmethod
    def _sanitise(s: str) -> str:
        # Only ASCII numbers, letters, '.', '-', and '_' are allowed in the local version label
        return re.sub(r'[^\w_\-\.]+', '.', s)


def get_version(cwd=None) -> str:
    """
    Analyses the source code to determine the pipeline version.

    @return: version string
    """
    cwd_saferun = functools.partial(_safe_run, cwd=cwd)

    commit_hash = cwd_saferun('git describe --always --tags --long --dirty')
    current_branch = cwd_saferun("git rev-parse --abbrev-ref HEAD")
    branch_hashes = cwd_saferun(
        f"git log --since 2019-10-01 --simplify-by-decoration --pretty=%H {current_branch}",
        on_error=''
    ).splitlines()
    refstags = cwd_saferun('git show-ref --tags -d', on_error='').splitlines()
    clean_workspace = cwd_saferun('git status -s -uno') == ''

    last_release_tag = _get_last_release_tag(current_branch, branch_hashes, refstags)
    # handle case where a release tag couldn't be identified, which can be artificially
    # manufactured by invalidating one or all of the current_branch, branch_hashes, or
    # refstags commands.
    if not last_release_tag:
        last_release_tag = '0.0.dev0'

    if current_branch in ['main', 'master'] or 'release/' in current_branch:
        last_branch_tag = last_release_tag
    else:
        last_branch_tag = _get_last_branch_tag(current_branch, branch_hashes, refstags)

    is_dirty = False
    if not clean_workspace or last_branch_tag == '':
        # No tag at all for branch
        is_dirty = True

    else:
        # Check if the latest tag is the latest commit
        headcommit = cwd_saferun('git rev-parse HEAD')
        tagcommit = cwd_saferun(f'git rev-list -n 1 {last_branch_tag}')
        if tagcommit != headcommit != 'N/A':
            is_dirty = True

    # If no Git commit info could be found, then attempt to load version
    # from the _version module that is created when pipeline package is
    # built.
    if commit_hash == "N/A":
        try:
            from pipeline._version import version
            return version
        except ModuleNotFoundError:
            last_release_tag = "0.0.dev0"

    return VersionInfo(
        commit_hash=commit_hash,
        branch=current_branch,
        branch_tag=last_branch_tag,
        release_tag=last_release_tag,
        dirty=is_dirty
    ).pipeline_version


def _to_number(value):
    try:
        return int(value)
    except ValueError:
        return 0

def _get_last_tag(gitbranch, branchpattern, delim, hashes, refstags):
    branchpattern = re.compile(branchpattern)
    delim = re.compile(delim)
    releaseid = gitbranch.split('/')[-1] if 'release/' in gitbranch else ''
    versions = {}
    for githash in hashes:
        for line in refstags:
            # if 'release/' is not in gitbranch, the second condition is always true
            if line.startswith(githash) and releaseid in line:
                tag = line.replace(githash+' refs/tags/', '').replace('^{}', '')
                if branchpattern.match(tag):
                    # split the version string into a tuple by the given delimiter,
                    # then convert each element of the tuple from string to int
                    # in order to sort the versions correctly.
                    # "versions" is a dict in which keys are the parsed tuples of ints,
                    # and values are the original version strings
                    versions[tuple(_to_number(x) for x in delim.split(tag))] = tag
    # sort the dict items by the first element (tuples of ints)
    # and return the second element (the corresponding version string)
    # of the highest value (i.e. most recent version)
    return sorted(versions.items())[-1][1] if versions else ''

def _get_last_branch_tag(gitbranch, hashes, refstags):
    match = re.match(r'(PIPE-\d+)', gitbranch)
    if match:
        branchpattern = match.group(1) + r'-\d+'
    else:
        branchpattern = gitbranch + r'-\d+'
    return _get_last_tag(gitbranch, branchpattern, '-', hashes, refstags)

def _get_last_release_tag(gitbranch, hashes, refstags):
    return _get_last_tag(gitbranch, r'^\d+\.\d+\.\d+\.\d+$', r'\.', hashes, refstags)


if __name__ == '__main__':
    print(get_version())
