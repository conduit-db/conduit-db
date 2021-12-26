@echo off
@rem "to specify default python version to 3.9 create/edit ~/AppData/Local/py.ini with [default] set
@rem to python3=3.9"
set TLD=%ScriptDir%\..
set CONDUIT_RAW_DIR=%~dp0..\conduit_raw
set CONDUIT_INDEX_DIR=%~dp0..\conduit_index
set CONDUIT_LIB_DIR=%~dp0..\conduit_lib
py -3.10 -m pip install pylint -U
py -3.10 -m pylint --rcfile ../.pylintrc %CONDUIT_RAW_DIR% %CONDUIT_INDEX_DIR% %CONDUIT_LIB_DIR%
