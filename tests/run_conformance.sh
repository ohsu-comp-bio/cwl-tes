#!/usr/bin/env bash

BDIR="$(cd `dirname $0`; pwd)"

if [ -n "$1" ]; then
    TEST=-n$1
fi

pushd $BDIR/schemas
./run_test.sh $TEST RUNNER=$BDIR/../cwl-tes-wrapper DRAFT=v1.0
## cleanup tmp dirs from execution
rm -rf v1.0[0-9a-zA-Z_]*
popd
