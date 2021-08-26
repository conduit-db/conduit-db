import sys
from typing import List, Any

from electrumsv_node import electrumsv_node


def cast_str_int_args_to_int(node_args: List[Any]) -> List[Any]:
    int_indices = []
    for index, arg in enumerate(node_args):

        if isinstance(arg, str) and arg.isdigit():
            int_indices.append(index)
        elif isinstance(arg, int):
            int_indices.append(index)

    for i in int_indices:
        node_args[i] = int(node_args[i])
    return node_args


def cast_str_bool_args_to_bool(node_args: List[Any]) -> List[Any]:
    false_indices = []
    for index, arg in enumerate(node_args):
        if isinstance(arg, str) and arg in {'false', "False"}:
            false_indices.append(index)
    for i in false_indices:
        node_args[i] = False

    true_indices = []
    for index, arg in enumerate(node_args):
        if isinstance(arg, str) and arg in {'true', "True"}:
            true_indices.append(index)
    for i in true_indices:
        node_args[i] = True

    return node_args


def call_any():
    if len(sys.argv[1:]) != 0:
        method_name = sys.argv[1]
        args = sys.argv[2:]
        if len(args) == 0:
            print(electrumsv_node.call_any(method_name).json()['result'])
        else:
            args = cast_str_bool_args_to_bool(args)
            args = cast_str_int_args_to_int(args)
            print(electrumsv_node.call_any(method_name, *args).json()['result'])


if __name__ == "__main__":
    call_any()
