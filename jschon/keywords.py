import re
from typing import *

from jschon.evaluation import EvaluationNode
from jschon.exceptions import JSONSchemaError, VocabularyError, URIError
from jschon.json import *
from jschon.jsonschema import *
from jschon.types import tuplify, arrayify
from jschon.uri import URI

__all__ = [
    # core vocabulary
    'SchemaKeyword',
    'VocabularyKeyword',
    'IdKeyword',
    'RefKeyword',
    'AnchorKeyword',
    'RecursiveRefKeyword',
    'RecursiveAnchorKeyword',
    'DefsKeyword',
    'CommentKeyword',

    # applicator vocabulary
    'AllOfKeyword',
    'AnyOfKeyword',
    'OneOfKeyword',
    'NotKeyword',
    'IfKeyword',
    'ThenKeyword',
    'ElseKeyword',
    'DependentSchemasKeyword',
    'ItemsKeyword',
    'AdditionalItemsKeyword',
    'UnevaluatedItemsKeyword',
    'ContainsKeyword',
    'PropertiesKeyword',
    'PatternPropertiesKeyword',
    'AdditionalPropertiesKeyword',
    'UnevaluatedPropertiesKeyword',
    'PropertyNamesKeyword',

    # validation vocabulary
    'TypeKeyword',
    'EnumKeyword',
    'ConstKeyword',
    'MultipleOfKeyword',
    'MaximumKeyword',
    'ExclusiveMaximumKeyword',
    'MinimumKeyword',
    'ExclusiveMinimumKeyword',
    'MaxLengthKeyword',
    'MinLengthKeyword',
    'PatternKeyword',
    'MaxItemsKeyword',
    'MinItemsKeyword',
    'UniqueItemsKeyword',
    'MaxContainsKeyword',
    'MinContainsKeyword',
    'MaxPropertiesKeyword',
    'MinPropertiesKeyword',
    'RequiredKeyword',
    'DependentRequiredKeyword',

    # format vocabulary
    'FormatKeyword',

    # meta-data vocabulary
    'TitleKeyword',
    'DescriptionKeyword',
    'DefaultKeyword',
    'DeprecatedKeyword',
    'ReadOnlyKeyword',
    'WriteOnlyKeyword',
    'ExamplesKeyword',

    # content vocabulary
    'ContentMediaTypeKeyword',
    'ContentEncodingKeyword',
    'ContentSchemaKeyword',
]


class SchemaKeyword(Keyword):
    __keyword__ = "$schema"
    __schema__ = {
        "type": "string",
        "format": "uri"
    }

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        if self.superschema.superkeyword is not None:
            raise JSONSchemaError('The "$schema" keyword must not appear in a subschema')

        try:
            (uri := URI(value)).validate(require_scheme=True, require_normalized=True)
        except URIError as e:
            raise JSONSchemaError from e

        self.superschema.metaschema_uri = uri


class VocabularyKeyword(Keyword):
    __keyword__ = "$vocabulary"
    __schema__ = {
        "type": "object",
        "propertyNames": {
            "type": "string",
            "format": "uri"
        },
        "additionalProperties": {
            "type": "boolean"
        }
    }

    def __init__(
            self,
            value: JSONObject[JSONBoolean],
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        if self.superschema.superkeyword is not None:
            raise JSONSchemaError('The "$vocabulary" keyword must not appear in a subschema')

        for vocab_uri, vocab_required in value.items():
            try:
                (vocab_uri := URI(vocab_uri)).validate(require_scheme=True, require_normalized=True)
            except URIError as e:
                raise JSONSchemaError from e

            if not isinstance(vocab_required, bool):
                raise JSONSchemaError('"$vocabulary" values must be booleans')

            try:
                vocabulary = Vocabulary(vocab_uri, vocab_required)
                self.superschema.kwclasses.update(vocabulary.kwclasses)
            except VocabularyError as e:
                if vocab_required:
                    raise JSONSchemaError(f"The metaschema requires an unrecognized vocabulary '{vocab_uri}'") from e


class IdKeyword(Keyword):
    __keyword__ = "$id"
    __schema__ = {
        "type": "string",
        "format": "uri-reference",
        "$comment": "Non-empty fragments not allowed.",
        "pattern": "^[^#]*#?$"
    }

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        (uri := URI(value)).validate(require_normalized=True, allow_fragment=False)
        if not uri.is_absolute():
            if not self.superschema.path:
                raise JSONSchemaError('The "$id" of the root schema, if present, must be an absolute URI')
            if (base_uri := self.superschema.base_uri) is not None:
                uri = uri.resolve(base_uri)
            else:
                raise JSONSchemaError(f'No base URI against which to resolve the "$id" value "{value}"')

        self.superschema.uri = uri


class RefKeyword(Keyword):
    __keyword__ = "$ref"
    __schema__ = {
        "type": "string",
        "format": "uri-reference"
    }

    def __call__(self, node: EvaluationNode) -> None:
        uri = URI(self.json.value)
        if not uri.can_absolute():
            if (base_uri := self.superschema.base_uri) is not None:
                uri = uri.resolve(base_uri)
            else:
                raise JSONSchemaError(f'No base URI against which to resolve the "$ref" value "{uri}"')

        refschema = JSONSchema.get(uri, self.superschema.metaschema_uri)
        refschema(node)


class AnchorKeyword(Keyword):
    __keyword__ = "$anchor"
    __schema__ = {
        "type": "string",
        "pattern": "^[A-Za-z][-A-Za-z0-9.:_]*$"
    }

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        if (base_uri := self.superschema.base_uri) is not None:
            uri = URI(f'{base_uri}#{value}')
        else:
            raise JSONSchemaError(f'No base URI for anchor "{value}"')

        JSONSchema.set(uri, self.superschema)


class RecursiveRefKeyword(Keyword):
    __keyword__ = "$recursiveRef"
    __schema__ = {
        "type": "string",
        "format": "uri-reference"
    }

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        if value != '#':
            raise JSONSchemaError('The "$recursiveRef" keyword may only take the value "#"')

    def __call__(self, node: EvaluationNode) -> None:
        if (base_uri := self.superschema.base_uri) is not None:
            refschema = JSONSchema.get(base_uri, self.superschema.metaschema_uri)
        else:
            raise JSONSchemaError(f'No base URI against which to resolve "$recursiveRef"')

        # if (recursive_anchor := refschema.keywords.get(RecursiveAnchorKeyword.__keyword__)) and \
        #         recursive_anchor.json.value is True:
        #     base_uri = ...

        refschema(node)


class RecursiveAnchorKeyword(Keyword):
    __keyword__ = "$recursiveAnchor"
    __schema__ = {
        "type": "boolean",
        "const": True,
        "default": False
    }


class DefsKeyword(Keyword):
    __keyword__ = "$defs"
    __schema__ = {
        "type": "object",
        "additionalProperties": {"$recursiveRef": "#"},
        "default": {}
    }
    applicators = PropertyApplicator,


class CommentKeyword(Keyword):
    __keyword__ = "$comment"
    __schema__ = {"type": "string"}


class AllOfKeyword(Keyword):
    __keyword__ = "allOf"
    __schema__ = {
        "type": "array",
        "minItems": 1,
        "items": {"$recursiveRef": "#"}
    }

    applicators = ArrayApplicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONArray[JSONSchema]

        for i, subschema in enumerate(self.json):
            if child := node.descend(node.json, subschema, key=str(i)):
                if not child.valid:
                    node.fail("The instance must be valid against all subschemas")
                    return

        node.pass_()


class AnyOfKeyword(Keyword):
    __keyword__ = "anyOf"
    __schema__ = {
        "type": "array",
        "minItems": 1,
        "items": {"$recursiveRef": "#"}
    }

    applicators = ArrayApplicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONArray[JSONSchema]

        node.fail("The instance must be valid against at least one subschema")
        for i, subschema in enumerate(self.json):
            if child := node.descend(node.json, subschema, key=str(i)):
                if child.valid:
                    node.pass_()


class OneOfKeyword(Keyword):
    __keyword__ = "oneOf"
    __schema__ = {
        "type": "array",
        "minItems": 1,
        "items": {"$recursiveRef": "#"}
    }

    applicators = ArrayApplicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONArray[JSONSchema]

        for i, subschema in enumerate(self.json):
            node.descend(node.json, subschema, key=str(i))

        if len([child for child in node.children.values() if child.valid]) == 1:
            node.pass_()
        else:
            node.fail("The instance must be valid against exactly one subschema")


class NotKeyword(Keyword):
    __keyword__ = "not"
    __schema__ = {"$recursiveRef": "#"}

    applicators = Applicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONSchema
        self.json(node)

        if not node.valid:
            node.pass_()
        else:
            node.fail("The instance must not be valid against the given subschema")


class IfKeyword(Keyword):
    __keyword__ = "if"
    __schema__ = {"$recursiveRef": "#"}

    applicators = Applicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONSchema
        self.json(node)
        node.assert_ = False


class ThenKeyword(Keyword):
    __keyword__ = "then"
    __schema__ = {"$recursiveRef": "#"}
    __depends__ = "if"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONSchema
        if (if_ := node.sibling("if")) and if_.valid:
            self.json(node)


class ElseKeyword(Keyword):
    __keyword__ = "else"
    __schema__ = {"$recursiveRef": "#"}
    __depends__ = "if"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONSchema
        if (if_ := node.sibling("if")) and not if_.valid:
            self.json(node)


class DependentSchemasKeyword(Keyword):
    __keyword__ = "dependentSchemas"
    __schema__ = {
        "type": "object",
        "additionalProperties": {"$recursiveRef": "#"}
    }
    __types__ = "object"

    applicators = PropertyApplicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONObject[JSONSchema]

        annotation = []
        for name, subschema in self.json.items():
            if name in node.json:
                if child := node.descend(node.json, subschema, key=name):
                    if child.valid:
                        annotation += [name]
                    else:
                        node.fail(f'The instance is invalid against the "{name}" subschema')
                        return

        node.pass_(annotation)


class ItemsKeyword(Keyword):
    __keyword__ = "items"
    __schema__ = {
        "anyOf": [
            {"$recursiveRef": "#"},
            {
                "type": "array",
                "minItems": 1,
                "items": {"$recursiveRef": "#"}
            }
        ]
    }
    __types__ = "array"

    applicators = Applicator, ArrayApplicator

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        if len(node.json) == 0:
            node.pass_()

        elif isinstance(self.json, JSONBooleanSchema):
            self.json(node)

        elif isinstance(self.json, JSONSchema):
            for index, item in enumerate(node.json):
                if child := node.descend(item, self.json):
                    if not child.valid:
                        node.fail(f"Array element {index} is invalid")
                        return

            node.pass_(True)

        elif isinstance(self.json, JSONArray):
            self.json: JSONArray[JSONSchema]
            annotation = None
            for index, item in enumerate(node.json[:len(self.json)]):
                annotation = index
                if child := node.descend(item, self.json[index], key=str(index)):
                    if not child.valid:
                        node.fail(f"Array element {index} is invalid")
                        return

            node.pass_(annotation)


class AdditionalItemsKeyword(Keyword):
    __keyword__ = "additionalItems"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "array"
    __depends__ = "items"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONSchema

        if (items := node.sibling("items")) and isinstance(items.annotation, int):
            annotation = None
            for index, item in enumerate(node.json[items.annotation + 1:]):
                annotation = True
                if child := node.descend(item, self.json):
                    if not child.valid:
                        node.fail(f"Array element {index} is invalid")
                        return

            node.pass_(annotation)


class UnevaluatedItemsKeyword(Keyword):
    __keyword__ = "unevaluatedItems"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "array"
    __depends__ = "items", "additionalItems", "if", "then", "else", "allOf", "anyOf", "oneOf", "not"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONSchema

        last_evaluated_item = -1
        for items_annotation in node.parent.annotations("items"):
            if items_annotation is True:
                return
            if isinstance(items_annotation, int) and items_annotation > last_evaluated_item:
                last_evaluated_item = items_annotation

        for additional_items_annotation in node.parent.annotations("additionalItems"):
            if additional_items_annotation is True:
                return

        for unevaluated_items_annotation in node.parent.annotations("unevaluatedItems"):
            if unevaluated_items_annotation is True:
                return

        annotation = None
        for index, item in enumerate(node.json[last_evaluated_item + 1:]):
            annotation = True
            if child := node.descend(item, self.json):
                if not child.valid:
                    node.fail(f"Array element {index} is invalid")
                    return

        node.pass_(annotation)


class ContainsKeyword(Keyword):
    __keyword__ = "contains"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "array"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONSchema

        annotation = 0
        for index, item in enumerate(node.json):
            if child := node.descend(item, self.json):
                if child.valid:
                    annotation += 1

        if annotation > 0:
            node.pass_(annotation)
        else:
            node.fail("The array does not contain a required element")


class PropertiesKeyword(Keyword):
    __keyword__ = "properties"
    __schema__ = {
        "type": "object",
        "additionalProperties": {"$recursiveRef": "#"},
        "default": {}
    }
    __types__ = "object"

    applicators = PropertyApplicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONObject[JSONSchema]

        annotation = []
        for name, item in node.json.items():
            if name in self.json:
                if child := node.descend(item, self.json[name], key=name):
                    if child.valid:
                        annotation += [name]
                    else:
                        node.fail(f'Object property "{name}" is invalid')
                        return

        node.pass_(annotation)


class PatternPropertiesKeyword(Keyword):
    __keyword__ = "patternProperties"
    __schema__ = {
        "type": "object",
        "additionalProperties": {"$recursiveRef": "#"},
        "propertyNames": {"format": "regex"},
        "default": {}
    }
    __types__ = "object"

    applicators = PropertyApplicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONObject[JSONSchema]

        matched_names = set()
        for name, item in node.json.items():
            for regex, subschema in self.json.items():
                if re.search(regex, name) is not None:
                    if child := node.descend(item, subschema, key=regex):
                        if child.valid:
                            matched_names |= {name}
                        else:
                            node.fail(f'Object property "{name}" is invalid')
                            return

        node.pass_(list(matched_names))


class AdditionalPropertiesKeyword(Keyword):
    __keyword__ = "additionalProperties"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "object"
    __depends__ = "properties", "patternProperties"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONSchema

        evaluated_names = set()
        if properties := node.sibling("properties"):
            evaluated_names |= set(properties.annotation or ())
        if pattern_properties := node.sibling("patternProperties"):
            evaluated_names |= set(pattern_properties.annotation or ())

        annotation = []
        for name, item in node.json.items():
            if name not in evaluated_names:
                if child := node.descend(item, self.json):
                    if child.valid:
                        annotation += [name]
                    else:
                        node.fail(f'Object property "{name}" is invalid')
                        return

        node.pass_(annotation)


class UnevaluatedPropertiesKeyword(Keyword):
    __keyword__ = "unevaluatedProperties"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "object"
    __depends__ = "properties", "patternProperties", "additionalProperties", \
                  "if", "then", "else", "dependentSchemas", \
                  "allOf", "anyOf", "oneOf", "not"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONSchema

        evaluated_names = set()
        for properties_annotation in node.parent.annotations("properties"):
            evaluated_names |= set(properties_annotation)
        for pattern_properties_annotation in node.parent.annotations("patternProperties"):
            evaluated_names |= set(pattern_properties_annotation)
        for additional_properties_annotation in node.parent.annotations("additionalProperties"):
            evaluated_names |= set(additional_properties_annotation)
        for unevaluated_properties_annotation in node.parent.annotations("unevaluatedProperties"):
            evaluated_names |= set(unevaluated_properties_annotation)

        annotation = []
        for name, item in node.json.items():
            if name not in evaluated_names:
                if child := node.descend(item, self.json):
                    if child.valid:
                        annotation += [name]
                    else:
                        node.fail(f'Object property "{name}" is invalid')
                        return

        node.pass_(annotation)


class PropertyNamesKeyword(Keyword):
    __keyword__ = "propertyNames"
    __schema__ = {"$recursiveRef": "#"}
    __types__ = "object"

    applicators = Applicator,

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONSchema

        for name in node.json:
            if child := node.descend(JSON(name), self.json):
                if not child.valid:
                    node.fail(f'Object property name "{name}" is invalid')
                    return

        node.pass_()


class TypeKeyword(Keyword):
    __keyword__ = "type"
    __schema__ = {
        "anyOf": [
            {"enum": ["null", "boolean", "number", "integer", "string", "array", "object"]},
            {
                "type": "array",
                "items": {"enum": ["null", "boolean", "number", "integer", "string", "array", "object"]},
                "minItems": 1,
                "uniqueItems": True
            }
        ]
    }

    def __init__(
            self,
            value: Union[str, Sequence[str]],
            **kwargs: Any,
    ) -> None:
        super().__init__(arrayify(value), **kwargs)

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONArray[JSONString]
        if any(node.json.istype(item.value) for item in self.json):
            node.pass_()
        else:
            node.fail(f"The value must be of type {self.json}")


class EnumKeyword(Keyword):
    __keyword__ = "enum"
    __schema__ = {"type": "array", "items": True}

    def __call__(self, node: EvaluationNode) -> None:
        self.json: JSONArray
        if node.json in self.json:
            node.pass_()
        else:
            node.fail(f"The value must be one of {self.json}")


class ConstKeyword(Keyword):
    __keyword__ = "const"
    __schema__ = True

    def __call__(self, node: EvaluationNode) -> None:
        if node.json == self.json:
            node.pass_()
        else:
            node.fail(f"The value must be equal to {self.json}")


class MultipleOfKeyword(Keyword):
    __keyword__ = "multipleOf"
    __schema__ = {"type": "number", "exclusiveMinimum": 0}
    __types__ = "number"

    def __call__(self, node: EvaluationNode[JSONNumber]) -> None:
        self.json: JSONNumber
        if node.json % self.json == 0:
            node.pass_()
        else:
            node.fail(f"The value must be a multiple of {self.json}")


class MaximumKeyword(Keyword):
    __keyword__ = "maximum"
    __schema__ = {"type": "number"}
    __types__ = "number"

    def __call__(self, node: EvaluationNode[JSONNumber]) -> None:
        self.json: JSONNumber
        if node.json <= self.json:
            node.pass_()
        else:
            node.fail(f"The value may not be greater than {self.json}")


class ExclusiveMaximumKeyword(Keyword):
    __keyword__ = "exclusiveMaximum"
    __schema__ = {"type": "number"}
    __types__ = "number"

    def __call__(self, node: EvaluationNode[JSONNumber]) -> None:
        self.json: JSONNumber
        if node.json < self.json:
            node.pass_()
        else:
            node.fail(f"The value must be less than {self.json}")


class MinimumKeyword(Keyword):
    __keyword__ = "minimum"
    __schema__ = {"type": "number"}
    __types__ = "number"

    def __call__(self, node: EvaluationNode[JSONNumber]) -> None:
        self.json: JSONNumber
        if node.json >= self.json:
            node.pass_()
        else:
            node.fail(f"The value may not be less than {self.json}")


class ExclusiveMinimumKeyword(Keyword):
    __keyword__ = "exclusiveMinimum"
    __schema__ = {"type": "number"}
    __types__ = "number"

    def __call__(self, node: EvaluationNode[JSONNumber]) -> None:
        self.json: JSONNumber
        if node.json > self.json:
            node.pass_()
        else:
            node.fail(f"The value must be greater than {self.json}")


class MaxLengthKeyword(Keyword):
    __keyword__ = "maxLength"
    __schema__ = {"type": "integer", "minimum": 0}
    __types__ = "string"

    def __call__(self, node: EvaluationNode[JSONString]) -> None:
        self.json: JSONInteger
        if len(node.json) <= self.json:
            node.pass_()
        else:
            node.fail(f"The text is too long (maximum {self.json} characters)")


class MinLengthKeyword(Keyword):
    __keyword__ = "minLength"
    __schema__ = {"type": "integer", "minimum": 0, "default": 0}
    __types__ = "string"

    def __call__(self, node: EvaluationNode[JSONString]) -> None:
        self.json: JSONInteger
        if len(node.json) >= self.json:
            node.pass_()
        else:
            node.fail(f"The text is too short (minimum {self.json} characters)")


class PatternKeyword(Keyword):
    __keyword__ = "pattern"
    __schema__ = {"type": "string", "format": "regex"}
    __types__ = "string"

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        self.regex = re.compile(value)

    def __call__(self, node: EvaluationNode[JSONString]) -> None:
        if self.regex.search(node.json.value) is not None:
            node.pass_()
        else:
            node.fail(f"The text must match the regular expression {self.json}")


class MaxItemsKeyword(Keyword):
    __keyword__ = "maxItems"
    __schema__ = {"type": "integer", "minimum": 0}
    __types__ = "array"

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONInteger
        if len(node.json) <= self.json:
            node.pass_()
        else:
            node.fail(f"The array has too many elements (maximum {self.json})")


class MinItemsKeyword(Keyword):
    __keyword__ = "minItems"
    __schema__ = {"type": "integer", "minimum": 0, "default": 0}
    __types__ = "array"

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONInteger
        if len(node.json) >= self.json:
            node.pass_()
        else:
            node.fail(f"The array has too few elements (minimum {self.json})")


class UniqueItemsKeyword(Keyword):
    __keyword__ = "uniqueItems"
    __schema__ = {"type": "boolean", "default": False}
    __types__ = "array"

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONBoolean
        if not self.json.value:
            node.pass_()
            return

        uniquified = []
        for item in node.json:
            if item not in uniquified:
                uniquified += [item]

        if len(node.json) == len(uniquified):
            node.pass_()
        else:
            node.fail("The array's elements must all be unique")


class MaxContainsKeyword(Keyword):
    __keyword__ = "maxContains"
    __schema__ = {"type": "integer", "minimum": 0}
    __types__ = "array"
    __depends__ = "contains"

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONInteger
        if contains := node.sibling("contains"):
            if contains.annotation <= self.json:
                node.pass_()
            else:
                node.fail('The array has too many elements matching the '
                          f'"contains" subschema (maximum {self.json})')


class MinContainsKeyword(Keyword):
    __keyword__ = "minContains"
    __schema__ = {"type": "integer", "minimum": 0, "default": 1}
    __types__ = "array"
    __depends__ = "contains", "maxContains"

    def __call__(self, node: EvaluationNode[JSONArray]) -> None:
        self.json: JSONInteger
        if contains := node.sibling("contains"):
            valid = contains.annotation >= self.json

            if valid and not contains.valid:
                max_contains = node.sibling("maxContains")
                if not max_contains or max_contains.valid:
                    contains.valid = True

            if valid:
                node.pass_()
            else:
                node.fail('The array has too few elements matching the '
                          f'"contains" subschema (minimum {self.json})')


class MaxPropertiesKeyword(Keyword):
    __keyword__ = "maxProperties"
    __schema__ = {"type": "integer", "minimum": 0}
    __types__ = "object"

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONInteger
        if len(node.json) <= self.json:
            node.pass_()
        else:
            node.fail(f"The object has too many properties (maximum {self.json})")


class MinPropertiesKeyword(Keyword):
    __keyword__ = "minProperties"
    __schema__ = {"type": "integer", "minimum": 0, "default": 0}
    __types__ = "object"

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONInteger
        if len(node.json) >= self.json:
            node.pass_()
        else:
            node.fail(f"The object has too few properties (minimum {self.json})")


class RequiredKeyword(Keyword):
    __keyword__ = "required"
    __schema__ = {
        "type": "array",
        "items": {"type": "string"},
        "uniqueItems": True,
        "default": []
    }
    __types__ = "object"

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONArray[JSONString]

        missing = [name for name in self.json if name.value not in node.json]
        if not missing:
            node.pass_()
        else:
            node.fail(f"The object is missing required properties {missing}")


class DependentRequiredKeyword(Keyword):
    __keyword__ = "dependentRequired"
    __schema__ = {
        "type": "object",
        "additionalProperties": {
            "type": "array",
            "items": {"type": "string"},
            "uniqueItems": True,
            "default": []
        }
    }
    __types__ = "object"

    def __call__(self, node: EvaluationNode[JSONObject]) -> None:
        self.json: JSONObject[JSONArray[JSONString]]

        missing = {}
        for name, dependents in self.json.items():
            if name in node.json:
                missing_deps = [dep for dep in dependents if dep.value not in node.json]
                if missing_deps:
                    missing[name] = missing_deps

        if not missing:
            node.pass_()
        else:
            node.fail(f"The object is missing dependent properties {missing}")


class FormatKeyword(Keyword):
    __keyword__ = "format"
    __schema__ = {"type": "string"}

    def __init__(
            self,
            value: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(value, **kwargs)
        vocabulary = FormatVocabulary.get(self.vocabulary_uri)
        self.format_: Optional[Format] = vocabulary.formats.get(self.json.value)

    def __call__(self, node: EvaluationNode) -> None:
        node.assert_ = self.format_.assert_ if self.format_ else False
        node.pass_(self.json)
        if self.format_ and isinstance(node.json, tuple(JSON.classfor(t) for t in tuplify(self.format_.__types__))):
            fmtresult = self.format_.evaluate(node.json)
            if not fmtresult.valid:
                node.fail(f'The text does not conform to the {self.json} format: {fmtresult.error}')


class TitleKeyword(Keyword):
    __keyword__ = "title"
    __schema__ = {"type": "string"}

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class DescriptionKeyword(Keyword):
    __keyword__ = "description"
    __schema__ = {"type": "string"}

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class DefaultKeyword(Keyword):
    __keyword__ = "default"
    __schema__ = True

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class DeprecatedKeyword(Keyword):
    __keyword__ = "deprecated"
    __schema__ = {
        "type": "boolean",
        "default": False
    }

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class ReadOnlyKeyword(Keyword):
    __keyword__ = "readOnly"
    __schema__ = {
        "type": "boolean",
        "default": False
    }

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class WriteOnlyKeyword(Keyword):
    __keyword__ = "writeOnly"
    __schema__ = {
        "type": "boolean",
        "default": False
    }

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class ExamplesKeyword(Keyword):
    __keyword__ = "examples"
    __schema__ = {
        "type": "array",
        "items": True
    }

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class ContentMediaTypeKeyword(Keyword):
    __keyword__ = "contentMediaType"
    __schema__ = {"type": "string"}

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class ContentEncodingKeyword(Keyword):
    __keyword__ = "contentEncoding"
    __schema__ = {"type": "string"}

    def __call__(self, node: EvaluationNode) -> None:
        node.pass_(self.json.value)


class ContentSchemaKeyword(Keyword):
    __keyword__ = "contentSchema"
    __schema__ = {"$recursiveRef": "#"}
    __depends__ = "contentMediaType"

    def __call__(self, node: EvaluationNode) -> None:
        if node.sibling("contentMediaType"):
            node.pass_(self.json.value)
