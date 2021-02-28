cd ..
set PYTHONPATH=.
py-spy record --subprocesses -o profile.svg -- python conduit_raw/conduit_server.py --reset
