"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
from setuptools import find_packages, setup
from Cython.Build import cythonize
from Cython.Compiler import Options
import pathlib

"""
To compile cython module:
> py -3.9-64 setup.py build_ext --inplace --build-lib .\conduit\workers

Ensure you use the corresponding version of python otherwise it won't work
"""

Options.annotate = True
Options.docstrings = True
Options.fast_fail = True

path = pathlib.Path('conduit_index/__init__.py')

with path.open() as f:
    for line in f.readlines():
        if line.startswith('__version__'):
            version = line.strip().split('= ')[1].strip("'")
            break

setup(
    name='conduit_index',
    version=version,
    description='Horizontally scalable and shardable transaction parser (depends on ConduitRaw)',
    long_description=open('../README.md', 'r').read(),
    long_description_content_type='text/markdown',
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
        'Programming Language :: Python :: Implementation :: PyPy'
    ],

    install_requires=[],
    extras_require={},
    tests_require=['pytest'],
    ext_modules=cythonize("conduit_index/workers/_algorithms.pyx"),
    zip_safe=False,
    packages=find_packages(),
)
