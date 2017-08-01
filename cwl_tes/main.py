from __future__ import absolute_import, print_function, unicode_literals

import cwltool.main
import sys
import logging

from cwl_tes.tes import TESPipeline

log = logging.getLogger("tes-backend")
log.setLevel(logging.DEBUG)
console = logging.StreamHandler()
log.addHandler(console)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = cwltool.main.arg_parser()
    parser = add_args(parser)
    parsed_args = parser.parse_args(args)
    if not len(args) >= 1:
        parser.print_help()
        return 1

    if parsed_args.tes is not None:
        pipeline = TESPipeline(parsed_args.tes, vars(parsed_args))
        rc = cwltool.main.main(
            args=parsed_args,
            executor=pipeline.executor,
            makeTool=pipeline.make_tool
        )
    else:
        rc = cwltool.main.main(
            args=parsed_args
        )
    return rc


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
