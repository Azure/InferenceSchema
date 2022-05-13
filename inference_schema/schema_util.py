# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import copy

from inference_schema._constants import INPUT_SCHEMA_ATTR, OUTPUT_SCHEMA_ATTR, ALL_SUPPORTED_VERSIONS

__functions_schema__ = {}
__versions__ = {}


def get_input_schema(func):
    """
    Extract the swagger input schema model from the decorated function.

    :param func:
    :type func: function | FunctionWrapper
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


def get_supported_versions(func):
    """
    Extract supported swagger versions from the decorated function. This will return the min set of supported
    versions between any decorators provided to the specified function. This could result in an empty list
    (if there is no overlap in versions between decorators), and it is ultimately up to the caller to decide
    how that case should be handled when creating the swagger document.

    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: list
    """
    decorators = _get_decorators(func)
    func_base_name = _get_function_full_qual_name(decorators[-1])

    input_versions = __versions__.get(func_base_name, {}).get(INPUT_SCHEMA_ATTR, {}).get('versions', [])
    output_versions = __versions__.get(func_base_name, {}).get(OUTPUT_SCHEMA_ATTR, {}).get('versions', [])
    if input_versions and output_versions:
        set_intersection = set(input_versions) & set(output_versions)
    elif input_versions:
        set_intersection = set(input_versions)
    elif output_versions:
        set_intersection = set(output_versions)
    else:
        set_intersection = ALL_SUPPORTED_VERSIONS
    return sorted(list(set_intersection))


def get_schemas_dict():
    """
    Retrieve a deepcopy of the dictionary that is used to track the provided function schemas

    :return:
    :rtype: dict
    """
    return copy.deepcopy(__functions_schema__)


def is_schema_decorated(func):
    """
    Check if a function is schema decorated

    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: boolean
    """
    decorators = _get_decorators(func)
    func_base_name = _get_function_full_qual_name(decorators[-1])
    return func_base_name in __functions_schema__


def _get_decorators(func):
    """
    Gets a list if decorators applied to a function

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
    """
    Gets the function name (original function name) + module

    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: str
    """

    original_func = _get_decorators(func)[-1]
    base_func_name = original_func.__name__
    module_name = getattr(original_func, '__module__', '<unknown>')
    return '{}.{}'.format(module_name, base_func_name)


def _get_schema_from_dictionary(attr, func):
    """
    Extract the schema specified on attr from the function schema dict

    :param attr:
    :type attr: str
    :param func:
    :type func: function | FunctionWrapper
    :return:
    :rtype: dict
    """

    schema = {"type": "object"}

    decorators = _get_decorators(func)
    func_base_name = _get_function_full_qual_name(decorators[-1])

    return __functions_schema__.get(func_base_name, {}).get(attr, schema)
