# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import io
from setuptools import setup
from setuptools import find_packages

VERSION = '0.1a0'

NAME = 'inference-schema'

DESCRIPTION = 'A project to add function decorators to allow for API swagger generation and schema type enforcement.'

DEPENDENCIES = [
    'python_dateutil>=2.5.3',
    'pytz>=2017.2',
    'wrapt==1.11.1'
]

EXTRAS = {
    'numpy-support': ['numpy>=1.13.0'],
    'pandas-support': ['pandas>=0.20.2'],
    'pyspark-support': ['pyspark==2.3.1']
}

CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
]

with io.open('LICENSE.txt', 'r', encoding='utf-8') as f:
    LICENSE = f.read()

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    classifiers=CLASSIFIERS,
    author='Microsoft Corp',
    license=LICENSE,

    install_requires=DEPENDENCIES,
    extras_require=EXTRAS,

    packages=find_packages(exclude=['*.tests']),
    include_package_data=True
)