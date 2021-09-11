import subprocess
import sys

process = subprocess.Popen(['docker', 'exec', 'conduit_node_1', 'bash', '-c', f'python3 /opt/call_any.py {" ".join(sys.argv[1:])}'],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
print(process.stdout.read())
