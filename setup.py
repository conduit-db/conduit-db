"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>

This setup.py is only for compiling the Cython extensions.
To run ConduitIndex and ConduitRaw see the README.md
"""

from setuptools import find_packages, setup
from Cython.Build import cythonize
from Cython.Compiler import Options

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
    name='conduitdb',
    version=version,
    description='A faster multi-core chain indexer using the bitcoin p2p protocol (Bitcoin SV)',
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
    ext_modules=cythonize("conduit_lib/_algorithms.pyx"),
    zip_safe=False,
    packages=find_packages(),
)
