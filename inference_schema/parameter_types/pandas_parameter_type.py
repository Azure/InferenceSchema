# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import pandas as pd
import numpy as np
from .abstract_parameter_type import AbstractParameterType
from ._swagger_from_dtype import Dtype2Swagger


class PandasParameterType(AbstractParameterType):
    """
    Class used to specify an expected parameter as a Pandas type.
    """

    def __init__(self, sample_input, enforce_column_type=True, enforce_shape=True, apply_column_names=True):
        """
        Construct the PandasParameterType object.

        :param sample_input: A sample input dataframe. This sample will be used as a basis for column types and array
            shape.
        :type sample_input: pd.DataFrame
        :param enforce_column_type:
        :type enforce_column_type: bool
        :param enforce_shape:
        :type enforce_shape: bool
        :param apply_column_names:
        :type apply_column_names: bool
        """
        if not isinstance(sample_input, pd.DataFrame):
            raise Exception("Invalid sample input provided, must provide a sample Pandas Dataframe.")

        super(PandasParameterType, self).__init__(sample_input)
        self.enforce_column_type = enforce_column_type
        self.enforce_shape = enforce_shape
        self.apply_column_names = apply_column_names

    def deserialize_input(self, input_data):
        """
        Convert the provided pandas-like object into a pandas dataframe. Will attempt to enforce column type and array shape
        as specified when constructed.

        :param input_data: The pandas-like object to convert.
        :type input_data: list | dict
        :return: The converted pandas dataframe.
        :rtype: np.DataFrame
        """

        if isinstance(input_data, pd.DataFrame):
            return input_data

        if not isinstance(input_data, list) and not isinstance(input_data, dict):
            raise Exception("Error, unable to convert input of type {} into Pandas Dataframe".format(type(input_data)))

        data_frame = pd.DataFrame(data=input_data)

        if self.apply_column_names and isinstance(input_data, list) and not isinstance(input_data[0], dict):
            data_frame.columns = self.sample_input.columns

        if self.enforce_column_type:
            sample_input_column_types = self.sample_input.dtypes.to_dict()
            converted_types = {x:sample_input_column_types.get(x, object) for x in data_frame.columns}
            data_frame = data_frame.astype(dtype=converted_types, copy=False)

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
        
        # Construct internal schema
        shape = self.sample_input.shape
        columns = self.sample_input.columns.values.tolist()
        types = self.sample_input.dtypes.tolist()

        col_count = len(columns)
        df_record_swagger = {'type': 'object', 'properties': {}}

        for i in range(col_count):
            """
            For string columns, Pandas tries to keep a uniform item size
            for the support ndarray, and such it stores references to strings
            instead of the string's bytes themselves, which have variable size.
            Because of this, even if the data is a string, the column's dtype is
            marked as 'object' since the reference is an object.

            We try to be smart about this here and if the column type is reported as
            object, we will also check the actual data in the column to see if its not
            actually a string, such that we can generate a better swagger schema later on.
            """
            col_name = columns[i]
            col_dtype = types[i]
            if col_dtype.name == 'object' and type(self.sample_input[columns[i]][0]) is str:
                col_dtype = np.dtype('str')
            col_swagger_type = Dtype2Swagger.convert_dtype_to_swagger(col_dtype)
            df_record_swagger['properties'][col_name] = col_swagger_type

        items_count = len(self.sample_input)
        sample_swagger = PandasParameterType._get_swagger_sample(self.sample_input.iloc, items_count, df_record_swagger)

        swagger_schema = {'type': 'array', 'items': df_record_swagger, 'example': sample_swagger}

        return swagger_schema
