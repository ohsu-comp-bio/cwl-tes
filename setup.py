#!/usr/bin/env python

from __future__ import print_function

import os
import sys

from setuptools import setup, find_packages


# if python3 runtime and `setup.py install` is called
if sys.version_info.major == 3 and sys.argv[1] == 'install':
    print("Aborting installation. cwl-tes doesn't support Python 3 currently.")
    print("Install using Python 2")
    exit(1)

SETUP_DIR = os.path.dirname(__file__)
README = os.path.join(SETUP_DIR, 'README.md')

setup(
    name='cwl-tes',
    version='0.1.0',
    description='Common workflow language reference implementation backended \
    by a GA4GH Task Execution Service',
    long_description=open(README).read(),
    author='Adam Struck',
    author_email='strucka@ohsu.edu',
    url="https://github.com/common-workflow-language/cwl-tes",
    license='Apache 2.0',
    packages=find_packages(),
    python_requires='>=2.6, <3',
    install_requires=[
        'cwltool==1.0.20170723124118',
        'py-tes>=0.1.2',
        'requests>=2.14.2'
    ],
    entry_points={
        'console_scripts': ["cwl-tes=cwl_tes.main:main"]},
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
    ],
)
