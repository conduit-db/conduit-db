# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import os
from pathlib import Path

# Constants
MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
REPO_PATH = MODULE_DIR.parent
COPYRIGHT_NOTICE = (
    "# Copyright (c) 2020-2023, Hayden Donnelly\n"
    "#\n"
    "# All rights reserved.\n"
    "#\n"
    "# Licensed under the MIT License; see LICENCE for details.\n\n"
)


def prepend_notice(file_path):
    with open(file_path, 'r+') as file:
        content = file.read()
        file.seek(0, 0)
        file.write(COPYRIGHT_NOTICE.rstrip('\r\n') + '\n\n' + content)


def main():
    for root, dirs, files in os.walk(REPO_PATH):
        for name in files:
            if name.endswith('.py'):
                file_path = os.path.join(root, name)
                prepend_notice(file_path)
                print(f"Updated {file_path}")


if __name__ == "__main__":
    main()
