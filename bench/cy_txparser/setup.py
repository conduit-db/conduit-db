from setuptools import setup
from Cython.Build import cythonize
from Cython.Compiler import Options

Options.annotate = True
Options.docstrings = True
Options.fast_fail = True

setup(
    name='preprocessor bench',
    ext_modules=cythonize("txparser.pyx"),
    zip_safe=False,
)
