from conduit_lib.utils import is_docker

if is_docker():
    SERVER_HOST = "0.0.0.0"
else:
    SERVER_HOST = "127.0.0.1"
SERVER_PORT = 34525
