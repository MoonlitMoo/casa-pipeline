"""
The pipeline.infrastructure.utils.subprocess module contains functions for
running commands in subprocesses and capturing the output.
"""

# The version.py module called in setup.py requires access to run and safe_run
# functions. These functions cannot be imported from a pipeline.utils module,
# as version.py must be executable as a standalone module. As a compromise,
# the subprocess utility code reside in version.py and are imported into this
# module, which is where all other pipeline code should access it from.
from pipeline.infrastructure.version import _run as run
from pipeline.infrastructure.version import _safe_run as safe_run

__all__ = ["run", "safe_run"]
