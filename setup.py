#!/usr/bin/env python

from __future__ import print_function

import io
import os
import re

from setuptools import setup, find_packages


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="cwl-tes",
    version=find_version("cwl_tes", "__init__.py"),
    description="Common workflow language reference implementation backended \
    by a GA4GH Task Execution Service",
    author="Adam Struck",
    author_email="strucka@ohsu.edu",
    url="https://github.com/common-workflow-language/cwl-tes",
    license="Apache 2.0",
    packages=find_packages(),
    python_requires=">=2.7, <4",
    install_requires=[
        "cwltool==1.0.20170727112954",
        "future>=0.16.0",
        "py-tes>=0.1.5",
        "requests>=2.14.2"
    ],
    entry_points={
        "console_scripts": ["cwl-tes=cwl_tes.main:main"]},
    zip_safe=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
