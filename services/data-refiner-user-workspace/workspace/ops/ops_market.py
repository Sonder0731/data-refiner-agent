from importlib import import_module
from pkgutil import walk_packages

from workspace import ops


OPS_SET = [
    operator
    for module_info in walk_packages(ops.__path__, f"{ops.__name__}.")
    if not module_info.ispkg and module_info.name != __name__
    for operator in vars(import_module(module_info.name)).values()
    if isinstance(operator, type)
    and operator.__module__ == module_info.name
    and getattr(operator, "__operator_type__", None) == "PROCESSING"
]