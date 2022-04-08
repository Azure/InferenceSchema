# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import pandas as pd

from pyspark.sql.session import SparkSession
from inference_schema.schema_util import get_supported_versions


class TestSparkParameterType(object):

    def test_spark_handling(self, decorated_spark_func):
        spark_session = SparkSession.builder.getOrCreate()
        spark_input_data = {'name': ['Sarah'], 'state': ['WA']}
        spark_input = spark_session.createDataFrame(pd.DataFrame(spark_input_data))
        state = spark_input.select('state')

        result = decorated_spark_func(spark_input)
        assert state.subtract(result).count() == result.subtract(state).count() == 0

        spark_input = [{'name': 'Sarah', 'state': 'WA'}]
        result = decorated_spark_func(spark_input)
        assert state.subtract(result).count() == result.subtract(state).count() == 0

        spark_input = {'param': [{'name': 'Sarah', 'state': 'WA'}]}
        result = decorated_spark_func(**spark_input)
        assert state.subtract(result).count() == result.subtract(state).count() == 0

        version_list = get_supported_versions(decorated_spark_func)
        assert '2.0' in version_list
        assert '3.0' in version_list
        assert '3.1' in version_list
