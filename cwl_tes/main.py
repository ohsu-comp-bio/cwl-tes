from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import functools
import signal
import sys
import logging
import ftplib
import uuid
from typing import Text


import pkg_resources
from six.moves import urllib

import cwltool.main
from cwltool.main import init_job_order as original_init_job_order
from cwltool.executors import MultithreadedJobExecutor
from cwltool.resolver import ga4gh_tool_registries
from cwltool.pathmapper import visit_class

from .tes import make_tes_tool
from .__init__ import __version__
from .ftp import FtpFsAccess

log = logging.getLogger("tes-backend")
log.setLevel(logging.INFO)
console = logging.StreamHandler()
# formatter = logging.Formatter("[%(asctime)s]\t[%(levelname)s]\t%(message)s")
# console.setFormatter(formatter)
log.addHandler(console)

DEFAULT_TMP_PREFIX = "tmp"


def versionstring():
    pkg = pkg_resources.require("cwltool")
    if pkg:
        cwltool_ver = pkg[0].version
    else:
        cwltool_ver = "unknown"
    return "%s %s with cwltool %s" % (sys.argv[0], __version__, cwltool_ver)


def custom_init_job_order(*args, **kwargs):
    """Uploads input Files to FTP and rewrite the input object."""
    remote_storage_url = kwargs.pop('args', args[1]).remote_storage_url
    ftp_access = kwargs.pop("ftp_fs_access", FtpFsAccess(os.curdir))
    job_order_object = original_init_job_order(*args, **kwargs)
    if remote_storage_url:
        remote_storage_url = ftp_access.join(remote_storage_url, "inputs")
        visit_class(
            job_order_object, ("File"), functools.partial(
                ftp_upload, remote_storage_url, ftp_access))
    return job_order_object


def ftp_upload(base_url, fs_access, cwl_file):
    """Upload a File to the given FTP URL; update the location URL to match."""
    if "path" not in cwl_file and not (
            "location" in cwl_file and cwl_file["location"].startswith(
                "file:/")):
        return
    path = cwl_file.get("path", cwl_file["location"][6:])
    target_path = basename = os.path.basename(path)
    basedir = urllib.parse.urlparse(base_url).path
    if basedir:
        target_path = basedir + '/' + basename
    try:
        fs_access.mkdir(base_url)
    except ftplib.all_errors:
        pass
    if not fs_access.isdir(base_url):
        raise Exception(
            'Failed to create target directory "{}".'.format(base_url))
    cwl_file["location"] = base_url + '/' + basename
    cwl_file.pop("path", None)
    if fs_access.isfile(fs_access.join(base_url, basename)):
        log.warning("FTP upload, file %s already exists", basename)
    else:
        ftp = fs_access._connect(base_url)
        with open(path, mode="rb") as source:
            ftp.storbinary("STOR {}".format(target_path), source)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = arg_parser()
    parsed_args = parser.parse_args(args)

    if parsed_args.version:
        print(versionstring())
        return 0

    if parsed_args.tes is None:
        print(versionstring())
        parser.print_usage()
        print("cwl-tes: error: argument --tes is required")
        return 1

    if parsed_args.quiet:
        log.setLevel(logging.WARN)
    if parsed_args.debug:
        log.setLevel(logging.DEBUG)

    def signal_handler(*args):  # pylint: disable=unused-argument
        """setup signal handler"""
        log.info(
            "recieved control-c signal"
        )
        log.info(
            "terminating thread(s)..."
        )
        log.warning(
            "remote TES task(s) will keep running"
        )
        sys.exit(1)
    signal.signal(signal.SIGINT, signal_handler)

    ftp_cache = {}

    class CachingFtpFsAccess(FtpFsAccess):
        """Ensures that the FTP connection cache is shared."""
        def __init__(self, basedir):
            super(CachingFtpFsAccess, self).__init__(basedir, ftp_cache)
    ftp_fs_access = CachingFtpFsAccess(os.curdir)
    if parsed_args.remote_storage_url:
        parsed_args.remote_storage_url = ftp_fs_access.join(
            parsed_args.remote_storage_url, str(uuid.uuid4()))
    loading_context = cwltool.main.LoadingContext(vars(parsed_args))
    loading_context.construct_tool_object = functools.partial(
        make_tes_tool, url=parsed_args.tes,
        remote_storage_url=parsed_args.remote_storage_url)
    runtime_context = cwltool.main.RuntimeContext(vars(parsed_args))
    runtime_context.make_fs_access = CachingFtpFsAccess
    cwltool.main.init_job_order = functools.partial(
        custom_init_job_order, ftp_fs_access=ftp_fs_access)
    return cwltool.main.main(
        args=parsed_args,
        executor=MultithreadedJobExecutor(),
        loadingContext=loading_context,
        runtimeContext=runtime_context,
        versionfunc=versionstring,
        logger_handler=console
    )


def arg_parser():  # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(
        description='GA4GH TES executor for Common Workflow Language.')
    parser.add_argument("--tes", type=str, help="GA4GH TES Service URL.")
    parser.add_argument("--basedir", type=Text)
    parser.add_argument("--outdir",
                        type=Text, default=os.path.abspath('.'),
                        help="Output directory, default current directory")
    parser.add_argument("--remote-storage-url", type=str)
    envgroup = parser.add_mutually_exclusive_group()
    envgroup.add_argument(
        "--preserve-environment",
        type=Text,
        action="append",
        help="Preserve specific environment variable when "
        "running CommandLineTools.  May be provided multiple "
        "times.",
        metavar="ENVVAR",
        default=[],
        dest="preserve_environment")
    envgroup.add_argument(
        "--preserve-entire-environment",
        action="store_true",
        help="Preserve all environment variable when running "
        "CommandLineTools.",
        default=False,
        dest="preserve_entire_environment")

    parser.add_argument("--tmpdir-prefix", type=Text,
                        help="Path prefix for temporary directories",
                        default=DEFAULT_TMP_PREFIX)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--tmp-outdir-prefix",
        type=Text,
        help="Path prefix for intermediate output directories",
        default=DEFAULT_TMP_PREFIX)

    exgroup.add_argument(
        "--cachedir",
        type=Text,
        default="",
        help="Directory to cache intermediate workflow outputs to avoid "
        "recomputing steps."
    )

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--rm-tmpdir",
        action="store_true",
        default=True,
        help="Delete intermediate temporary directories (default)",
        dest="rm_tmpdir")

    exgroup.add_argument(
        "--leave-tmpdir",
        action="store_false",
        default=True,
        help="Do not delete intermediate temporary directories",
        dest="rm_tmpdir")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--move-outputs",
        action="store_const",
        const="move",
        default="move",
        help="Move output files to the workflow output directory and delete "
        "intermediate output directories (default).",
        dest="move_outputs")

    exgroup.add_argument(
        "--leave-outputs",
        action="store_const",
        const="leave",
        default="move",
        help="Leave output files in intermediate output directories.",
        dest="move_outputs")

    exgroup.add_argument(
        "--copy-outputs",
        action="store_const",
        const="copy",
        default="move",
        help="Copy output files to the workflow output directory, don't "
        "delete intermediate output directories.",
        dest="move_outputs")

    parser.add_argument(
        "--rdf-serializer",
        help="Output RDF serialization format used by --print-rdf (one of "
        "turtle (default), n3, nt, xml)",
        default="turtle")

    parser.add_argument(
        "--eval-timeout",
        help="Time to wait for a Javascript expression to evaluate before "
        "giving an error, default 20s.",
        type=float,
        default=20)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--print-rdf",
        action="store_true",
        help="Print corresponding RDF graph for workflow and exit")
    exgroup.add_argument(
        "--print-dot",
        action="store_true",
        help="Print workflow visualization in graphviz format and exit")
    exgroup.add_argument(
        "--print-pre",
        action="store_true",
        help="Print CWL document after preprocessing.")
    exgroup.add_argument(
        "--print-deps",
        action="store_true",
        help="Print CWL document dependencies.")
    exgroup.add_argument(
        "--print-input-deps",
        action="store_true",
        help="Print input object document dependencies.")
    exgroup.add_argument(
        "--pack",
        action="store_true",
        help="Combine components into single document and print.")
    exgroup.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit")
    exgroup.add_argument(
        "--validate",
        action="store_true",
        help="Validate CWL document only.")
    exgroup.add_argument(
        "--print-supported-versions",
        action="store_true",
        help="Print supported CWL specs.")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--strict",
        action="store_true",
        help="Strict validation (unrecognized or out of place fields are "
        "error)",
        default=True,
        dest="strict")
    exgroup.add_argument(
        "--non-strict",
        action="store_false",
        help="Lenient validation (ignore unrecognized fields)",
        default=True,
        dest="strict")

    parser.add_argument(
        "--skip-schemas",
        action="store_true",
        help="Skip loading of schemas",
        default=False,
        dest="skip_schemas")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--verbose", action="store_true", help="Default logging")
    exgroup.add_argument(
        "--quiet",
        action="store_true",
        help="Only print warnings and errors.")
    exgroup.add_argument(
        "--debug",
        action="store_true",
        help="Print even more logging")

    parser.add_argument(
        "--timestamps",
        action="store_true",
        help="Add timestamps to the errors, warnings, and notifications.")
    parser.add_argument(
        "--js-console",
        action="store_true",
        help="Enable javascript console output")
    parser.add_argument("--disable-js-validation",
                        action="store_true",
                        help="Disable javascript validation.")
    parser.add_argument("--js-hint-options-file",
                        type=Text,
                        help="File of options to pass to jshint."
                        "This includes the added option \"includewarnings\". ")

    parser.add_argument(
        "--tool-help",
        action="store_true",
        help="Print command line help for tool")

    parser.add_argument(
        "--relative-deps",
        choices=[
            'primary',
            'cwd'],
        default="primary",
        help="When using --print-deps, print paths "
        "relative to primary file or current working directory.")

    parser.add_argument(
        "--enable-dev",
        action="store_true",
        help="Enable loading and running development versions of CWL spec.",
        default=False)

    parser.add_argument(
        "--enable-ext",
        action="store_true",
        help="Enable loading and running cwltool extensions to CWL spec.",
        default=False)

    parser.add_argument(
        "--default-container",
        help="Specify a default docker container that will be used if the "
        "workflow fails to specify one.")
    parser.add_argument("--disable-validate", dest="do_validate",
                        action="store_false", default=True,
                        help=argparse.SUPPRESS)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--enable-ga4gh-tool-registry",
        action="store_true",
        help="Enable resolution using GA4GH tool registry API",
        dest="enable_ga4gh_tool_registry",
        default=True)
    exgroup.add_argument(
        "--disable-ga4gh-tool-registry",
        action="store_false",
        help="Disable resolution using GA4GH tool registry API",
        dest="enable_ga4gh_tool_registry",
        default=True)

    parser.add_argument(
        "--add-ga4gh-tool-registry",
        action="append",
        help="Add a GA4GH tool registry endpoint to use for resolution, "
        "default %s" % ga4gh_tool_registries,
        dest="ga4gh_tool_registries",
        default=[])

    parser.add_argument(
        "--on-error",
        help="Desired workflow behavior when a step fails. "
        "One of 'stop' or 'continue'. Default is 'stop'.",
        default="stop",
        choices=("stop", "continue"))

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--compute-checksum",
        action="store_true",
        default=True,
        help="Compute checksum of contents while collecting outputs",
        dest="compute_checksum")
    exgroup.add_argument(
        "--no-compute-checksum",
        action="store_false",
        help="Do not compute checksum of contents while collecting outputs",
        dest="compute_checksum")

    parser.add_argument(
        "--relax-path-checks",
        action="store_true",
        default=False,
        help="Relax requirements on path names to permit "
        "spaces and hash characters.",
        dest="relax_path_checks")
    parser.add_argument("--make-template",
                        action="store_true",
                        help="Generate a template input object")
    parser.add_argument(
        "--overrides",
        type=str,
        default=None,
        help="Read process requirement overrides from file.")

    parser.add_argument(
        "workflow",
        type=Text,
        nargs="?",
        default=None,
        metavar='cwl_document',
        help="path or URL to a CWL Workflow, "
        "CommandLineTool, or ExpressionTool. If the `inputs_object` has a "
        "`cwl:tool` field indicating the path or URL to the cwl_document, "
        " then the `workflow` argument is optional.")
    parser.add_argument(
        "job_order",
        nargs=argparse.REMAINDER,
        metavar='inputs_object',
        help="path or URL to a YAML or JSON "
        "formatted description of the required input values for the given "
        "`cwl_document`.")

    return parser


if __name__ == "__main__":
    sys.exit(main())
