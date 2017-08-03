from __future__ import print_function, unicode_literals

import glob
import os
import shutil
import subprocess
import sys
import time

from .funnel_test_util import SimpleServerTest, popen


class TestConformance(SimpleServerTest):

    def test_conformance(self):
        cwl_testdir = os.path.join(self.testdir, "schemas/v1.0")
        ctest_def = os.path.join(cwl_testdir, "conformance_test_v1.0.yaml")
        tool_entry = os.path.join(self.rootprojectdir, "cwl-tes")

        cmd = [
            "cwltest", "--test", ctest_def, "--basedir", self.tmpdir,
            "--tool", tool_entry, "-j", "20", "--",
            "--tes=http://localhost:8000"
        ]

        testlog = os.path.join(
            self.tmpdir,
            "conformance_test_results_%s.txt" % (time.strftime("%m%d%Y"))
        )
        print("RUNNNING:", " ".join(cmd))
        p = popen(
            cmd,
            cwd=os.path.join(self.testdir, "schemas/v1.0"),
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            bufsize=1
        )
        with p.stdout, open(testlog, 'ab') as fh:
            for line in iter(p.stdout.readline, b''):
                print(line.decode('utf8').strip("\n"), file=sys.stderr)
                fh.write(line)
        p.wait()

        ctest_dirs = glob.glob(cwl_testdir + "[a-zA-Z0-9_]*")
        cleanup_tmpdirs(ctest_dirs)

        assert p.returncode == 0


def cleanup_tmpdirs(*args):
    for d in args:
        if isinstance(d, list):
            for sd in d:
                shutil.rmtree(sd, True)
        else:
            shutil.rmtree(d, True)
