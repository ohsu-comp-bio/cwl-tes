from __future__ import absolute_import, print_function, unicode_literals

import argparse
import cwltool.main
import os
import pkg_resources
import signal
import sys
import logging

from cwltool.executors import MultithreadedJobExecutor
from cwltool.resolver import ga4gh_tool_registries
from typing import Text

from cwl_tes.tes import make_tes_tool
from cwl_tes.__init__ import __version__


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

    # setup signal handler
    def signal_handler(*args):
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

    return cwltool.main.main(
        args=parsed_args,
        executor=MultithreadedJobExecutor(),
        makeTool=make_tes_tool,
        versionfunc=versionstring,
        logger_handler=console
    )


def arg_parser():  # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(
        description='GA4GH TES executor for Common Workflow Language.')
    parser.add_argument("--tes", type=str, help="GA4GH TES Service URL.")
    parser.add_argument("--basedir", type=Text)
    parser.add_argument("--outdir", type=Text, default=os.path.abspath('.'),
                        help="Output directory, default current directory")
    envgroup = parser.add_mutually_exclusive_group()
    envgroup.add_argument(
        "--preserve-environment",
        type=Text,
        action="append",
        help="Preserve specific environment variable when "
        "running CommandLineTools.  May be provided multiple "
        "times.",
        metavar="ENVVAR",
        default=["PATH"],
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
        "--verbose",
        action="store_true",
        help="Default logging")
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
    exgroup.add_argument("--make-template", action="store_true",
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
