# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import sys


def edit_env_file(file_path, variable, new_value):
    # Read the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Modify the content
    found = False
    for i, line in enumerate(lines):
        if line.startswith(variable + "="):
            lines[i] = f"{variable}={new_value}\n"
            found = True
            break

    # If the variable was not found, append it
    if not found:
        print(f"VARIABLE: '{variable}' not found")

    # Write the changes back
    with open(file_path, 'w') as file:
        file.writelines(lines)


# Usage
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <path_to_env_file> <variable> <new_value>")
        sys.exit(1)

    file_path = sys.argv[1]
    variable = sys.argv[2]
    new_value = sys.argv[3]

    edit_env_file(file_path, variable, new_value)
