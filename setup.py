from setuptools import find_packages, setup
import pathlib

path = pathlib.Path('__init__.py')

with path.open() as f:
    for line in f.readlines():
        if line.startswith('__version__'):
            version = line.strip().split('= ')[1].strip("'")
            break

setup(
    name='conduit',
    version=version,
    description='zero-copy, multi-core bitcoin chain indexer',
    long_description=open('README.rst', 'r').read(),
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

    packages=find_packages(),
)
