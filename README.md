[![Build Status](https://travis-ci.org/ohsu-comp-bio/cwl-tes.svg?branch=master)](https://travis-ci.org/ohsu-comp-bio/cwl-tes)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# GA4GH CWL Task Execution

___cwl-tes___ submits your tasks to a TES server. Task submission is parallelized when possible.


## Requirements

* Python 2.7 / 3.5 / 3.6

* [Docker](https://docs.docker.com/)


## Quickstart

How to run a CWL workflow on the EBRAINS experimental TES server:

1. Clone this repo:

        git clone git@gitlab.ebrains.eu:technical-coordination/private/workflows/cwl-tes.git


2. Install this version of the cwl-tes package:

        # create a virtualenv:
        python3 -m virtualenv cwl-env
        source cwl-env/bin/activate
        
        # or venv:
        python3 -m venv cwl-env
        source cwl-env/bin/activate
        pip install --upgrade pip
        pip install --upgrade wheel

        # and install cwl-tes:
        cd cwl-tes/
        pip install .


3. Make sure that you have access to the CSCS Swift object storage and that your credentials are stored correctly in ~/.aws/credentials:

        [default]
        aws_access_key_id=EXAMPLE_KEY_ID
        aws_secret_access_key=EXAMPLE_ACCESS_KEY

    For info on how to get the access key id and secret access key, see here: https://user.cscs.ch/storage/object_storage/#swift-s3-api


4. Obtain an EBRAINS token (from the Collaboratory):

         export token=EXAMPLE_TOKEN

5. Find a CWL workflow and run it using cwl-tes:

        cwl-tes --tes <tes-endpoint> --remote-storage-url <object-storage-endpoint>/<container_name> --token $token <workflow>.cwl <workflow_info>.yml

For example:

        cwl-tes --tes https://tes-codejam.apps-dev.hbp.eu/  --remote-storage-url https://swift.bsc.es/tesk_storage_container --token $token workflow.cwl workflow_info.yml
