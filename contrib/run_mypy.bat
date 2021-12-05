@echo off
@rem "to specify default python version to 3.9 create/edit ~/AppData/Local/py.ini with [default] set
@rem to python3=3.9"

REM Get current folder with no trailing slash
SET ScriptDir=%~dp0
SET TLD=%ScriptDir%\..
echo %ScriptDir%
cd %ScriptDir%
mypy --config=%TLD%\mypy.ini %TLD%\conduit_raw
mypy --config=%TLD%\mypy.ini %TLD%\conduit_index
mypy --config=%TLD%\mypy.ini %TLD%\conduit_lib
