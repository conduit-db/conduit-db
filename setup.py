from setuptools import find_packages, setup
from Cython.Build import cythonize
from Cython.Compiler import Options
import pathlib

"""
To compile cython module:
> py -3.8-64 setup.py build_ext --inplace
"""

Options.annotate = True
Options.docstrings = True
Options.fast_fail = True

path = pathlib.Path('conduit/__init__.py')

with path.open() as f:
    for line in f.readlines():
        if line.startswith('__version__'):
            version = line.strip().split('= ')[1].strip("'")
            break

setup(
    name='conduit',
    version=version,
    description='zero-copy, multi-core bitcoin chain indexer',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    author='AustEcon',
    author_email='AustEcon0922@gmail.com',
    maintainer='AustEcon',
    maintainer_email='AustEcon0922@gmail.com',
    url='https://github.com/AustEcon/conduit',
    download_url='https://github.com/AustEcon/bitsv/tarball/{}'.format(version),
    license='MIT',

    keywords=[
        'conduit',
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
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],

    install_requires=[],
    extras_require={},
    tests_require=['pytest'],
    ext_modules=cythonize("conduit/_algorithms.pyx"),
    zip_safe=False,
    packages=find_packages(),
)
