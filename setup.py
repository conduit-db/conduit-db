"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
import os

from setuptools import find_packages, setup
from Cython.Build import cythonize
from Cython.Compiler import Options
import pathlib

version = "0.0.1"

"""
To compile cython module:
> py -3.9-64 setup.py build_ext --inplace

Ensure you use the corresponding version of python otherwise it won't work
"""

Options.annotate = True
Options.docstrings = True
Options.fast_fail = True

setup(
    name='conduit_raw',
    version=version,
    description='Very fast preprocessor for raw block data in the ConduitDB pipeline',
    author='AustEcon',
    author_email='AustEcon0922@gmail.com',
    maintainer='AustEcon',
    maintainer_email='AustEcon0922@gmail.com',
    url='https://github.com/AustEcon/conduit',
    download_url='https://github.com/AustEcon/bitsv/tarball/{}'.format(version),

    keywords=[
        'conduit_index',
        'bsv',
        'bitcoin sv',
        'cryptocurrency',
        'payments',
        'tools',
        'wallet',
    ],

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
    ],

    install_requires=[],
    extras_require={},
    tests_require=['pytest'],
    ext_modules=cythonize("_algorithms.pyx"),
    zip_safe=False,
    packages=find_packages(),
)
