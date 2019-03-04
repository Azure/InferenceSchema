# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import copy
import inspect

from inference_schema._constants import INPUT_SCHEMA_ATTR, OUTPUT_SCHEMA_ATTR

__functions_schema__ = {}


def get_input_schema(func):
    """
    Extract the swagger input schema model from the decorated function.

    :param func:
    :type func: function | FunctionWrappere
    :return:
    :rtype: dict
    """

    return _get_schema_from_dictionary(INPUT_SCHEMA_ATTR, func)


def get_output_schema(func):
    """
    Extract the swagger output schema model from the decorated function.

    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: dict
    """

    return _get_schema_from_dictionary(OUTPUT_SCHEMA_ATTR, func)


def get_schemas_dict():
    return copy.deepcopy(__functions_schema__)


def is_schema_decorated(func):
    """
    Check if a function is schema decorated
    :param func:
    :type func: function | FunctionWrappere
    :return:
    :rtype: boolean
    """
    decorators = _get_decorators(func)
    func_base_name = _get_function_full_qual_name(decorators[-1])
    return func_base_name in __functions_schema__


def _get_decorators(func):
    """

    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: list
    """

    dec = []
    if not func.__closure__:
        return [func]
    if hasattr(func, '__closure__'):
        for closure in func.__closure__:
            dec.extend(_get_decorators(closure.cell_contents))
    return [func] + dec


def _get_function_full_qual_name(func):
    decorators = _get_decorators(func)
    base_func_name = decorators[-1].__name__
    module = inspect.getmodule(decorators[-1])
    module_name = "" if module is None else module.__name__
    return '{}.{}'.format(module_name, base_func_name)


def _get_schema_from_dictionary(attr, func):

    schema = {"type": "object"}

    decorators = _get_decorators(func)
    func_base_name = _get_function_full_qual_name(decorators[-1])

    return __functions_schema__.get(func_base_name, {}).get(attr, schema)

