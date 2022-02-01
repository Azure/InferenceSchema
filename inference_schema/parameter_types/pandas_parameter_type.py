# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import json
import pandas as pd
from .abstract_parameter_type import AbstractParameterType
from ._util import get_swagger_for_list, get_swagger_for_nested_dict
from ._constants import SWAGGER_FORMAT_CONSTANTS


class PandasParameterType(AbstractParameterType):
    """
    Class used to specify an expected parameter as a Pandas type.
    """

    def __init__(self, sample_input, enforce_column_type=True, enforce_shape=True, apply_column_names=False,
                 orient='records'):
        """
        Construct the PandasParameterType object.

        :param sample_input: A sample input dataframe. This sample will be used as a basis for column types and array
            shape.
        :type sample_input: pd.DataFrame
        :param enforce_column_type: Enforce that input column types much match those of the provided sample when
            `deserialize_input` is called.
        :type enforce_column_type: bool
        :param enforce_shape: Enforce that input shape must match that of the provided sample when `deserialize_input`
            is called.
        :type enforce_shape: bool
        :param apply_column_names: Apply column names from the provided sample onto the input when `deserialize_input`
            is called. Disabled by default, as there is no guaranteed order for dictionary keys, so it's possible for
            names to be applied in the wrong order. Recommended to only use if the expected input will be an array 
            representation of the dataframe.
        :type apply_column_names: bool
        :param orient: The Pandas orient to use when converting between a json object and a DataFrame. Possible orients
            are 'split', 'records', 'index', 'columns', 'values', or 'table'. More information about these orients can
            be found in the Pandas documentation for `to_json` and `read_json`.
        :type orient: string
        """
        if not isinstance(sample_input, pd.DataFrame):
            raise Exception("Invalid sample input provided, must provide a sample Pandas Dataframe.")

        super(PandasParameterType, self).__init__(sample_input)
        self.enforce_column_type = enforce_column_type
        self.enforce_shape = enforce_shape
        self.apply_column_names = apply_column_names

        if orient not in ('split', 'records', 'index', 'columns', 'values', 'table'):
            raise Exception("Invalid orient provided, must be one of ('split', 'records', 'index', 'columns', "
                            "'values', or 'table')")
        self.orient = orient

    def deserialize_input(self, input_data):
        """
        Convert the provided pandas-like object into a pandas dataframe. Will attempt to enforce column type and array
        shape as specified when constructed.

        :param input_data: The pandas-like object to convert.
        :type input_data: list | dict
        :return: The converted pandas dataframe.
        :rtype: np.DataFrame
        """

        if isinstance(input_data, pd.DataFrame):
            return input_data

        if not isinstance(input_data, list) and not isinstance(input_data, dict):
            raise Exception("Error, unable to convert input of type {} into Pandas Dataframe".format(type(input_data)))

        data_frame = pd.read_json(json.dumps(input_data), orient=self.orient)

        if self.apply_column_names:
            data_frame.columns = self.sample_input.columns.copy()

        if self.enforce_column_type:
            sample_input_column_types = self.sample_input.dtypes.to_dict()
            converted_types = {x: sample_input_column_types.get(x, object) for x in data_frame.columns}
            for column_name, column_type in converted_types.items():
                if str(column_type).startswith('timedelta'):
                    data_frame[column_name] = pd.to_timedelta(data_frame[column_name])
            data_frame = data_frame.astype(dtype=converted_types)

        if self.enforce_shape:
            expected_shape = self.sample_input.shape
            parsed_data_dims = len(data_frame.shape)
            expected_dims = len(expected_shape)
            if parsed_data_dims != expected_dims:
                raise ValueError(
                    "Invalid input data frame: a data frame with {0} dimensions is expected; "
                    "input has {1} [shape {2}]".format(expected_dims, parsed_data_dims, data_frame.shape))

            for dim in range(1, len(expected_shape)):
                if data_frame.shape[dim] != expected_shape[dim]:
                    raise ValueError(
                        "Invalid input data frame: data frame has size {0} on dimension #{1}, "
                        "while expected value is {2}".format(
                            data_frame.shape[dim], dim, expected_shape[dim]))

        return data_frame

    def input_to_swagger(self):
        """
        Generates a swagger schema for the provided sample pandas dataframe

        :return: The swagger schema object.
        :rtype: dict
        """
        LIST_LIKE_ORIENTS = ('records', 'values')
        json_sample = json.loads(self.sample_input.to_json(orient=self.orient, date_format='iso'))

        if self.orient in LIST_LIKE_ORIENTS:
            swagger_schema = get_swagger_for_list(json_sample)
        else:
            swagger_schema = get_swagger_for_nested_dict(json_sample)

        if self.orient == 'records':
            for column_name in self.sample_input.columns:
                data_type = str(self.sample_input.dtypes[column_name])
                if data_type.startswith('datetime'):
                    swagger_schema['items']['properties'][str(column_name)]['format'] = 'date-time'
                elif data_type.startswith('timedelta'):
                    swagger_schema['items']['properties'][str(column_name)]['format'] = 'timedelta'
        elif self.orient == 'index':
            for row in swagger_schema['properties'].values():
                for column_name in self.sample_input.columns:
                    data_type = str(self.sample_input.dtypes[column_name])
                    if data_type.startswith('datetime'):
                        row['properties'][str(column_name)]['format'] = 'date-time'
                    elif data_type.startswith('timedelta'):
                        row['properties'][str(column_name)]['format'] = 'timedelta'
        elif self.orient == 'columns':
            for column_name in self.sample_input.columns:
                for row_info in swagger_schema['properties'][str(column_name)]['properties'].values():
                    data_type = str(self.sample_input.dtypes[column_name])
                    if data_type.startswith('datetime'):
                        row_info['format'] = 'date-time'
                    elif data_type.startswith('timedelta'):
                        row_info['format'] = 'timedelta'
        elif self.orient == 'table':
            for column_name in self.sample_input.columns:
                data_type = str(self.sample_input.dtypes[column_name])
                if data_type.startswith('datetime'):
                    swagger_schema['properties']['data']['items']['properties'][str(column_name)]['format'] = \
                        'date-time'
                elif data_type.startswith('timedelta'):
                    swagger_schema['properties']['data']['items']['properties'][str(column_name)]['format'] = \
                        'timedelta'
        swagger_schema["format"] = SWAGGER_FORMAT_CONSTANTS.PANDAS_FORMAT.format(self.orient)
        return swagger_schema
