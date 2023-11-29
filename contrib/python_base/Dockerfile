# Use an official Python runtime as a parent image
FROM python:3.10

WORKDIR /usr/src/app

COPY . .

RUN pip install -r contrib/requirements.txt
RUN pip install -r contrib/requirements-dev.txt

# Run mypy checks
CMD ["mypy", "--config-file", "mypy.ini", "./conduit_raw", "--python-version", "3.10", "--namespace-packages", "--explicit-package-bases"]
CMD ["mypy", "--config-file", "mypy.ini", "./conduit_lib", "--python-version", "3.10", "--namespace-packages", "--explicit-package-bases"]
CMD ["mypy", "--config-file", "mypy.ini", "./conduit_index", "--python-version", "3.10", "--namespace-packages", "--explicit-package-bases"]
