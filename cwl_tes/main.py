from __future__ import absolute_import, print_function, unicode_literals

import cwltool.main
import pkg_resources
import signal
import sys
import logging

from cwl_tes.tes import TESPipeline
from cwl_tes.__init__ import __version__


log = logging.getLogger("tes-backend")
log.setLevel(logging.INFO)
console = logging.StreamHandler()
# formatter = logging.Formatter("[%(asctime)s]\t[%(levelname)s]\t%(message)s")
# console.setFormatter(formatter)
log.addHandler(console)


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

    parser = cwltool.main.arg_parser()
    parser = add_args(parser)
    parsed_args = parser.parse_args(args)

    if not len(args) >= 1:
        print(versionstring())
        print("CWL document required, no input file was provided")
        parser.print_usage()
        return 1

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

    blacklist_false = ["no_container", "disable_pull", "disable_net",
                       "custom_net", "no_match_user"]
    for f in blacklist_false:
        if vars(parsed_args).get(f):
            log.warning("arg: '%s' has no effect in cwl-tes" % (f))

    blacklist_true = ["enable_pull"]
    for f in blacklist_true:
        if not vars(parsed_args).get(f):
            log.warning("arg: '%s' has no effect in cwl-tes" % (f))

    # custom
    if not parsed_args.rm_container:
        log.warning("arg: 'leave_container' has no effect in cwl-tes")

    pipeline = TESPipeline(parsed_args.tes, vars(parsed_args))

    # setup signal handler
    def signal_handler(*args):
        log.info(
            "recieved control-c signal"
        )
        log.info(
            "terminating thread(s)..."
        )
        log.warning(
            "remote TES processes %s may keep running" %
            ([t.id for t in pipeline.threads])
        )
        sys.exit(1)
    signal.signal(signal.SIGINT, signal_handler)

    return cwltool.main.main(
        args=parsed_args,
        executor=pipeline.executor,
        makeTool=pipeline.make_tool,
        versionfunc=versionstring,
        logger_handler=console
    )


def add_args(parser):
    parser.add_argument(
        "--tes",
        type=str,
        help="GA4GH TES Service URL"
    )
    return parser


if __name__ == "__main__":
    sys.exit(main())
