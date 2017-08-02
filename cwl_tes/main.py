from __future__ import absolute_import, print_function, unicode_literals

import cwltool.main
import sys
import logging

from cwl_tes.tes import TESPipeline
from cwl_tes.__init__ import __version__


log = logging.getLogger("tes-backend")
log.setLevel(logging.INFO)
console = logging.StreamHandler()
log.addHandler(console)


def versionstring():
    return "%s version %s" % (sys.argv[0], __version__)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = cwltool.main.arg_parser()
    parser = add_args(parser)
    parsed_args = parser.parse_args(args)
    if not len(args) >= 1:
        parser.print_help()
        return 1

    if parsed_args.quiet:
        log.setLevel(logging.WARN)
    if parsed_args.debug:
        log.setLevel(logging.DEBUG)

    blacklist_false = ["no_container", "disable_pull", "disable_net",
                       "rm_container", "custom_net", "no_match_user"]
    for f in blacklist_false:
        if vars(parsed_args).get(f):
            log.warning("[warning] arg: %s has no effect in cwl-tes" % (f))

    blacklist_true = ["leave_container", "enable_pull"]
    for f in blacklist_true:
        if not vars(parsed_args).get(f):
            log.warning("[warning] arg: %s has no effect in cwl-tes" % (f))

    if parsed_args.tes is not None:
        pipeline = TESPipeline(parsed_args.tes, vars(parsed_args))
        rc = cwltool.main.main(
            args=parsed_args,
            executor=pipeline.executor,
            makeTool=pipeline.make_tool,
            versionfunc=versionstring
        )
        return rc
    else:
        log.error("You must provide a TES server URL!")
        return 1


def add_args(parser):
    parser.add_argument(
        "--tes",
        type=str,
        default="http://localhost:8000",
        help="GA4GH TES Service URL"
    )
    return parser


if __name__ == "__main__":
    sys.exit(main())
