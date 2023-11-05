@echo off
@rem "to specify default python version to 3.9 create/edit ~/AppData/Local/py.ini with [default] set
@rem to python3=3.9"

REM Get current folder with no trailing slash
set ScriptDir=%~dp0
set TLD=%ScriptDir%
set CONDUIT_RAW_DIR=%~dp0\conduit_raw
set CONDUIT_INDEX_DIR=%~dp0\conduit_index
set CONDUIT_LIB_DIR=%~dp0\conduit_lib
set CONDUIT_TESTS_DIR=%~dp0\tests

cd %ScriptDir%

py -3.10 -m pip install pylint -U

REM Run type checks
mypy --config=%TLD%\mypy.ini %TLD%\conduit_raw --python-version 3.10 --namespace-packages --explicit-package-bases
mypy --config=%TLD%\mypy.ini %TLD%\conduit_index --python-version 3.10 --namespace-packages --explicit-package-bases
mypy --config=%TLD%\mypy.ini %TLD%\conduit_lib --python-version 3.10 --namespace-packages --explicit-package-bases
mypy --config=%TLD%\mypy.ini %TLD%\tests --python-version 3.10 --namespace-packages --explicit-package-bases

REM Run pylint
set PYTHONPATH="."
py -3.10 -m pylint --rcfile ./.pylintrc %CONDUIT_LIB_DIR%
py -3.10 -m pylint --rcfile ./.pylintrc %CONDUIT_RAW_DIR%
py -3.10 -m pylint --rcfile ./.pylintrc %CONDUIT_INDEX_DIR%
py -3.10 -m pylint --rcfile ./.pylintrc %CONDUIT_TESTS_DIR%
