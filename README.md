[![Build Status](https://travis-ci.org/common-workflow-language/cwl-tes.svg?branch=master)](https://travis-ci.org/common-workflow-language/cwl-tes)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# GA4GH CWL Task Execution 

___cwl-tes___ submits your tasks to a TES server. Task submission is parallelized when possible.

[Funnel](https://ohsu-comp-bio.github.io/funnel) is an implementation of the [GA4GH task execution API](https://github.com/ga4gh/task-execution-schemas). It runs your dockerized tasks on slurm, htcondor, google compute engine, etc.


## Requirements

* Python 2.7

* [Docker](https://docs.docker.com/)

* [Funnel](https://ohsu-comp-bio.github.io/funnel)

## Quickstart

* Start the task server

```
funnel server
```

* Run your CWL tool/workflow

```
cwl-tes --tes http://localhost:8000 tests/hashsplitter-workflow.cwl.yml --input tests/resources/test.txt
```

## Install

To install from source:

```
python setup.py install
```


## Run the v1.0 conformance tests

To start a funnel server instance automatically and run all of the conformance tests:

```
nosetests -w ./tests
```


_A more manual approach:_

Start the funnel server.

```
funnel server --config /path/to/config.yaml
```

Make sure that TMPDIR is specified in the AllowedDirs of your Local storage configuration.

Run the tests.

```
./tests/run_conformance.sh
```
