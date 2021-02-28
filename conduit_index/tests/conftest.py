import os
from pathlib import Path

# because I'm not ready to configure logging properly right now (tests throw errors...)
if not Path("logs").exists():
    os.mkdir("logs")
