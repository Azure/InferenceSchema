# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import wrapt
import inspect
import copy
from functools import partial

from .schema_util import _get_decorators, _get_function_full_qual_name, __functions_schema__, __versions__
from .parameter_types.abstract_parameter_type import AbstractParameterType
from ._constants import INPUT_SCHEMA_ATTR, OUTPUT_SCHEMA_ATTR


def input_schema(param_name, param_type, convert_to_provided_type=True, optional=False):
    """
    Decorator to define an input schema model for a function parameter
    The input schema is a representation of what type the function expects
    and will also generates a swagger representation of that input, that can
    be used on a swagger api document.

    If 'convert_to_provided_type' is True, and the function receive as argument for
    that parameter a parsed json representation of the type (in the format specified
    in the swagger schema generated), then the argument will be converted to the
    expected type.

    :param param_name: name of the parameter which input schema is being specified
    :type param_name: str
    :param param_type: type of the parameter
    :type param_type: AbstractParameterType
    :param convert_to_provided_type:
    :type convert_to_provided_type: bool
    :return:
    :rtype:
    """

    if not isinstance(param_type, AbstractParameterType):
        raise Exception('Error, provided param_type must be a subclass' +
                        'of the AbstractParameterType.')

    swagger_schema = {param_name: param_type.input_to_swagger()}
    supported_versions = param_type.supported_versions()

    @_schema_decorator(attr_name=INPUT_SCHEMA_ATTR, schema=swagger_schema, supported_versions=supported_versions)
    def decorator_input(user_run, instance, args, kwargs):
        if convert_to_provided_type:
            args = list(args)

            if param_name not in kwargs.keys() and not optional:
                decorators = _get_decorators(user_run)
                arg_names = inspect.getfullargspec(decorators[-1]).args
                if param_name not in arg_names:
                    raise Exception('Error, provided param_name "{}" '
                                    'is not in the decorated function.'.format(param_name))
                param_position = arg_names.index(param_name)
                args[param_position] = _deserialize_input_argument(args[param_position], param_type, param_name)
            elif optional and (param_name not in kwargs.keys() or not kwargs[param_name]):
                pass
            else:
                kwargs[param_name] = _deserialize_input_argument(kwargs[param_name], param_type, param_name)

            args = tuple(args)

        return user_run(*args, **kwargs)

    return decorator_input


def output_schema(output_type):
    """
    Decorator to define an output schema model for a function parameter
    The output schema is a representation of type of data the function
    is expected to return.
    A swagger schema model will be generated for that out, that can be
    used on a swagger api document.

    :param output_type:
    :type output_type: AbstractParameterType
    :return:
    :rtype:
    """

    if not isinstance(output_type, AbstractParameterType):
        raise Exception('Error, provided param_type must be a subclass '
                        'of the AbstractParameterType.')

    swagger_schema = output_type.input_to_swagger()
    supported_versions = output_type.supported_versions()

    @_schema_decorator(attr_name=OUTPUT_SCHEMA_ATTR, schema=swagger_schema, supported_versions=supported_versions)
    def decorator_input(user_run, instance, args, kwargs):
        return user_run(*args, **kwargs)

    return decorator_input


# Heavily based on the wrapt.decorator implementation
def _schema_decorator(wrapper=None, enabled=None, attr_name=None, schema=None, supported_versions=None):
    """
    Decorator to generate decorators, preserving the metadata passed to the
    decorator arguments, that is needed to be able to extact that information
    at a later time, without disturbing the decorated function.
    This is modeled in the same way that the wrapt.decorator universal
    decorator

    :param wrapper:
    :type wrapper: function | None
    :param enabled:
    :type enabled: boolean | None
    :param attr_name:
    :type attr_name: str | None
    :param schema:
    :type schema: dict | None
    :param supported_versions:
    :type supported_versions: List | None
    :return:
    :rtype: function | FunctionWrapper
    """

    if wrapper is not None:
        def _build(wrapped, wrapper, enabled=None, user_function=None):
            return wrapt.FunctionWrapper(
                wrapped=wrapped,
                wrapper=wrapper,
                enabled=enabled
            )

        def _wrapper(wrapped, instance, args, kwargs):
            if instance is None and inspect.isclass(wrapped) and not args:
                def _capture(target_wrapped):
                    _enabled = enabled
                    if type(_enabled) is bool:
                        if not _enabled:
                            return target_wrapped
                        _enabled = None

                    target_wrapper = wrapped(**kwargs)

                    return _build(target_wrapped, target_wrapper, _enabled)
                return _capture

            _add_schema_to_global_schema_dictionary(attr_name, schema, args[0])
            _add_versions_to_global_versions_dictionary(attr_name, supported_versions, args[0])
            target_wrapped = args[0]

            _enabled = enabled
            if type(_enabled) is bool:
                if not _enabled:
                    return target_wrapped

                _enabled = None

            if instance is None:
                if inspect.isclass(wrapped):
                    target_wrapper = wrapped()

                else:
                    target_wrapper = wrapper

            else:
                if inspect.isclass(instance):
                    target_wrapper = wrapper.__get__(None, instance)

                else:
                    target_wrapper = wrapper.__get__(instance, type(instance))

            return _build(target_wrapped, target_wrapper)

        return _build(wrapper, _wrapper)
    else:
        return partial(
            _schema_decorator,
            enabled=enabled,
            attr_name=attr_name,
            schema=schema,
            supported_versions=supported_versions
        )


def _add_schema_to_global_schema_dictionary(attr_name, schema, user_func):
    """
    function to add a generated schema for 'attr_name', to the function schema dict

    :param attr_name:
    :type attr_name: str
    :param schema:
    :type schema: dict
    :param user_func:
    :type user_func: function | FunctionWrapper
    :return:
    :rtype:
    """

    if attr_name is None or schema is None:
        pass

    decorators = _get_decorators(user_func)
    base_func_name = _get_function_full_qual_name(decorators[-1])
    arg_names = inspect.getfullargspec(decorators[-1]).args

    if base_func_name not in __functions_schema__.keys():
        __functions_schema__[base_func_name] = {}

    if attr_name == INPUT_SCHEMA_ATTR:
        _add_input_schema_to_global_schema_dictionary(base_func_name, arg_names, schema)
    elif attr_name == OUTPUT_SCHEMA_ATTR:
        _add_output_schema_to_global_schema_dictionary(base_func_name, schema)
    else:
        pass


def _add_versions_to_global_versions_dictionary(attr_name, versions, user_func):
    """
    function to add supported swagger versions for 'attr_name', to the function versions dict

    :param attr_name:
    :type attr_name: str
    :param versions:
    :type versions: List
    :param user_func:
    :type user_func: function | FunctionWrapper
    :return:
    :rtype:
    """

    if attr_name is None or versions is None:
        pass

    decorators = _get_decorators(user_func)
    base_func_name = _get_function_full_qual_name(decorators[-1])

    if base_func_name not in __versions__.keys():
        __versions__[base_func_name] = {}

    if attr_name == INPUT_SCHEMA_ATTR or attr_name == OUTPUT_SCHEMA_ATTR:
        _add_attr_versions_to_global_schema_dictionary(base_func_name, versions, attr_name)
    else:
        pass


def _add_input_schema_to_global_schema_dictionary(base_func_name, arg_names, schema):
    """
    function to add a generated input schema, to the function schema dict

    :param base_func_name: function full qualified name
    :type base_func_name: str
    :param arg_names:
    :type arg_names: list
    :param schema:
    :type schema: dict
    :return:
    :rtype:
    """

    if INPUT_SCHEMA_ATTR not in __functions_schema__[base_func_name].keys():
        __functions_schema__[base_func_name][INPUT_SCHEMA_ATTR] = {
            "type": "object",
            "properties": {},
            "example": {}
        }

        for n in arg_names:
            __functions_schema__[base_func_name][INPUT_SCHEMA_ATTR]["properties"][n] = {}
            __functions_schema__[base_func_name][INPUT_SCHEMA_ATTR]["example"][n] = ""

    for k in schema:
        item_swagger = copy.deepcopy(schema[k])
        __functions_schema__[base_func_name][INPUT_SCHEMA_ATTR]["example"][k] = item_swagger["example"]
        del item_swagger["example"]
        __functions_schema__[base_func_name][INPUT_SCHEMA_ATTR]["properties"][k] = item_swagger


def _add_attr_versions_to_global_schema_dictionary(base_func_name, versions, attr):
    """
    function to add supported swagger versions to the version dict

    :param base_func_name: function full qualified name
    :type base_func_name: str
    :param versions:
    :type versions: list
    :param attr:
    :type attr: str
    :return:
    :rtype:
    """

    if attr not in __versions__[base_func_name].keys():
        __versions__[base_func_name][attr] = {
            "type": "object",
            "versions": {}
        }

    __versions__[base_func_name][attr]["versions"] = versions


def _add_output_schema_to_global_schema_dictionary(base_func_name, schema):
    """
    function to add a generated output schema, to the function schema dict

    :param base_func_name: function full qualified name
    :type base_func_name: str
    :param schema:
    :type schema: dict
    :return:
    :rtype:
    """

    if OUTPUT_SCHEMA_ATTR in __functions_schema__[base_func_name].keys():
        raise Exception('Error, output schema already defined for function: {}.'.format(base_func_name))

    __functions_schema__[base_func_name][OUTPUT_SCHEMA_ATTR] = schema


def _deserialize_input_argument(input_data, param_type, param_name):
    """
    function to deserialize / convert input to exact type described in schema

    :param input_data:
    :param param_type: subclass of AbstractParameterType
    :param param_name:
    :return:
    """
    sample_data_type = param_type.sample_data_type
    if sample_data_type is dict:
        if not isinstance(input_data, dict):
            raise ValueError("Invalid input data type to parse. Expected: {0} but got {1}".format(
                sample_data_type, type(input_data)))
        sample_data_type_map = param_type.sample_data_type_map
        # parameters other than subclass of AbstractParameterType will not be handled
        for k, v in sample_data_type_map.items():
            if k not in input_data.keys():
                continue
            input_data[k] = _deserialize_input_argument(input_data[k], v, k)
    elif sample_data_type in (list, tuple):
        sample_data_type_list = param_type.sample_data_type_list
        if not isinstance(input_data, list) and not isinstance(input_data, tuple):
            raise ValueError("Invalid input data type to parse. Expected: {0} but got {1}".format(
                sample_data_type, type(input_data)))
        # OpenAPI 2.x does not support mixed type in array
        if len(sample_data_type_list):
            input_data = [_deserialize_input_argument(x, sample_data_type_list[0], param_name) for x in input_data]
    else:
        # non-nested input will be deserialized
        if not isinstance(input_data, sample_data_type):
            input_data = param_type.deserialize_input(input_data)
    return input_data
