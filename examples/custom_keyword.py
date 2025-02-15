import pathlib
import pprint

from jschon import create_catalog, URI, JSON, JSONSchema, JSONSchemaError, LocalSource
from jschon.jsonschema import Result
from jschon.vocabulary import Keyword

data_dir = pathlib.Path(__file__).parent / 'data'

# cache of enumeration values obtained from remote terminology services
remote_enum_cache = {
    "https://example.com/remote-enum-colours": [
        "red",
        "orange",
        "yellow",
        "green",
        "blue",
        "indigo",
        "violet",
    ]
}


# define a class that implements the "enumRef" keyword
class EnumRefKeyword(Keyword):
    key = "enumRef"

    # ignore non-string instances
    instance_types = "string",

    def __init__(self, parentschema: JSONSchema, value: str):
        super().__init__(parentschema, value)

        # raise an exception during schema construction if a reference is invalid
        if value not in remote_enum_cache:
            raise JSONSchemaError(f"Unknown remote enumeration {value}")

    def evaluate(self, instance: JSON, result: Result) -> None:
        # the keyword's value is a reference to a remote enumeration
        enum_ref = self.json.value

        # evaluate the current JSON instance node against the enumeration
        if instance.data in remote_enum_cache.get(enum_ref):
            # (optionally) on success, annotate the result
            result.annotate(enum_ref)
        else:
            # on failure, mark the result as failed, with an (optional) error message
            result.fail(f"The instance is not a member of the {enum_ref} enumeration")


# initialize the catalog, with JSON Schema 2020-12 vocabulary support
catalog = create_catalog('2020-12')

# add a local source for loading the enumRef meta-schema and vocabulary
# definition files
catalog.add_uri_source(
    URI("https://example.com/enumRef/"),
    LocalSource(data_dir, suffix='.json'),
)

# implement the enumRef vocabulary using the EnumRefKeyword class
catalog.create_vocabulary(
    URI("https://example.com/enumRef"),
    EnumRefKeyword,
)

# create a schema for validating that a string is a member of a remote enumeration
schema = JSONSchema({
    "$schema": "https://example.com/enumRef/enumRef-metaschema",
    "$id": "https://example.com/remote-enum-test-schema",
    "type": "string",
    "enumRef": "https://example.com/remote-enum-colours",
})

# validate the schema against its meta-schema
schema_validity = schema.validate()
print(f'Schema validity check: {schema_validity.valid}')

# declare a valid JSON instance
valid_json = JSON("green")

# declare an invalid JSON instance
invalid_json = JSON("purple")

# evaluate the valid instance
valid_result = schema.evaluate(valid_json)

# evaluate the invalid instance
invalid_result = schema.evaluate(invalid_json)

# print output for the valid case
print(f'Valid JSON result: {valid_result.valid}')
print('Valid JSON detailed output:')
pprint.pp(valid_result.output('detailed'))

# print output for the invalid case
print(f'Invalid JSON result: {invalid_result.valid}')
print('Invalid JSON detailed output:')
pprint.pp(invalid_result.output('detailed'))
