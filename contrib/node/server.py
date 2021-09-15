import os
import subprocess

from electrumsv_node import electrumsv_node

extra_config_options = [
    "-debug=1",
    "-rejectmempoolrequest=0",
    "-rpcallowip=0.0.0.0/0",
    "-rpcbind=0.0.0.0",
]
split_command = electrumsv_node.shell_command(print_to_console=True, extra_params=extra_config_options)
process = subprocess.Popen(" ".join(split_command), shell=True, env=os.environ.copy())
process.wait()
