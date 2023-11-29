#!/bin/bash

# Run mypy checks
mypy --config-file mypy.ini ./conduit_raw --python-version 3.10 --namespace-packages --explicit-package-bases
mypy --config-file mypy.ini ./conduit_lib --python-version 3.10 --namespace-packages --explicit-package-bases
mypy --config-file mypy.ini ./conduit_index --python-version 3.10 --namespace-packages --explicit-package-bases

# Run pylint checks
python3 -m pylint --rcfile ./.pylintrc ./conduit_lib
python3 -m pylint --rcfile ./.pylintrc ./conduit_index
python3 -m pylint --rcfile ./.pylintrc ./conduit_raw
