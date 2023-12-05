# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import ruamel.yaml
import json

in_file = ".redocly.yaml"
out_file = ".redocly.json"

yaml = ruamel.yaml.YAML(typ="safe")
with open(in_file) as fi:
    yaml_object = yaml.load(fi)
with open(out_file, "w") as fo:
    json.dump(yaml_object["referenceDocs"], fo, separators=(",", ":"))
