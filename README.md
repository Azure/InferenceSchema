[![Integration Build](https://github.com/Azure/InferenceSchema/workflows/Integration%20Tests/badge.svg)](https://github.com/Azure/InferenceSchema/actions)

# InferenceSchema

This Python package is intended to provide a uniform schema for common machine learning applications, as well as a set
of decorators that can be used to aid in web based ML prediction applications.

## Features

### Function Decorators

The input and output decorators offered by this package are design to simplify the process of prediction in a web based
application. They each provide support for schema generation based on a provided sample input, with the idea being
that this schema can then be taken and embedded into an API Swagger specification. Additionally, the input decorator
provides support for type conversion at runtime from a JSON based input into the user specified sample type, to allow
for easy conversion of input over the wire into the datatype that the decorated function expects. These decorators can
be nested with each other and with other decorators as desired.

### Schema Generation

The package provides support for generating schema based on example input provided to the input and output decorators.
This is intended to introduce a uniform conversion between a JSON format that can be embedded into a swagger
specification and the in memory Python objects which may or may not have a built-in JSON representation.

### Type Conversion

The input decorator provides support to convert input that is passed to the decorated function from a JSON type into
the type specified by the provided sample. This allows the function to be called in a web based manner without needing
to convert the data from over the wire either prior to the function call or as a part of the function handling. If the
provided input is already of the sample type, the decorator is a no-op. Each currently supported type offers options
on how much conversion and input enforcement should be done at runtime. Currently the output decorator does no form
of type conversion.

### Supported Types

Currently the package supports generation for Numpy, Pandas, and Spark types, as well as standard Python types. These
types are defined [here](inference_schema/parameter_types). Custom types can be implemented by extending the 
[AbstractParameterType](inference_schema/parameter_types/abstract_parameter_type.py) and overriding the
`deserialize_input` and `input_to_swagger` methods.

## Samples

Some sample usage for the decorators and each of the supported types can be found in the 
[test resources](tests/resources/decorated_function_samples.py).

## Installing

This package is available for install via [PyPi](https://pypi.org/project/inference-schema). The package supports
dependency install via pip extras, so as to not bloat the user environment. Currently available extras are
'numpy-support', 'pandas-support', and 'spark-support'.

## Contributing

This project follows the Microsoft standard contribution guidelines. More information can be found 
[here](CONTRIBUTING.MD).

In order to work on this project, you will need a working Python 3 environment (virtualenv recommended). Package
dependencies are specified in [setup.py](setup.py), as well as package extras. These extras are defined separately to
allow users to install only the packages they want, to not bloat their environment.

## License

Please refer to [LICENSE](LICENSE.txt) for package licensing information.

## Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.