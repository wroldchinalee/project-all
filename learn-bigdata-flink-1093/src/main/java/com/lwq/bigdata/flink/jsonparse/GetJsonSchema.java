package com.lwq.bigdata.flink.jsonparse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.*;

/**
 * @author: LWQ
 * @create: 2020/12/24
 * @description: GetJsonSchema
 **/
public class GetJsonSchema {
    private GetJsonSchema() {
        // private
    }

    // see https://spacetelescope.github.io/understanding-json-schema/UnderstandingJSONSchema.pdf
    private static final String PROPERTIES = "properties";
    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String TYPE = "type";
    private static final String FORMAT = "format";
    private static final String CONTENT_ENCODING = "contentEncoding";
    private static final String ITEMS = "items";
    private static final String ADDITIONAL_ITEMS = "additionalItems";
    private static final String REF = "$ref";
    private static final String ALL_OF = "allOf";
    private static final String ANY_OF = "anyOf";
    private static final String NOT = "not";
    private static final String ONE_OF = "oneOf";

    // from https://tools.ietf.org/html/draft-zyp-json-schema-03#page-14
    private static final String DISALLOW = "disallow";
    private static final String EXTENDS = "extends";

    private static final String TYPE_NULL = "null";
    private static final String TYPE_BOOLEAN = "boolean";
    private static final String TYPE_OBJECT = "object";
    private static final String TYPE_ARRAY = "array";
    private static final String TYPE_NUMBER = "number";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_STRING = "string";

    private static final String FORMAT_DATE = "date";
    private static final String FORMAT_TIME = "time";
    private static final String FORMAT_DATE_TIME = "date-time";

    private static final String CONTENT_ENCODING_BASE64 = "base64";

    @SuppressWarnings("unchecked")
    public static <T> TypeInformation<T> convert(String jsonSchema) {
        Preconditions.checkNotNull(jsonSchema, "JSON schema");
        final ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory()
                .enable(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS)
                .enable(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
        final JsonNode node;
        try {
            node = mapper.readTree(jsonSchema);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Invalid JSON schema.", e);
        }
        return (TypeInformation<T>) convertType("<root>", node, node);
    }

    private static TypeInformation<?> convertType(String location, JsonNode node, JsonNode root) {
        // we use a set here to unify types (e.g. types that just add metadata such as 'multipleOf')
        final Set<TypeInformation<?>> typeSet = new HashSet<>();

        System.out.printf("location:%s\n", location);

        // use TYPE of this node
        if (node.has(TYPE)) {
            final JsonNode typeNode = node.get(TYPE);

            List<String> types = new ArrayList<>();
            // array of types
            if (typeNode.isArray()) {
                final Iterator<JsonNode> elements = typeNode.elements();
                while (elements.hasNext()) {
                    types.add(elements.next().asText());
                }
            }
            // single type
            else if (typeNode.isTextual()) {
                types.add(typeNode.asText());
            }

            for (String type : types) {
                // set field type
                switch (type) {
                    case TYPE_NULL:
                        typeSet.add(Types.VOID);
                        break;
                    case TYPE_BOOLEAN:
                        typeSet.add(Types.BOOLEAN);
                        break;
                    case TYPE_STRING:
                        if (node.has(FORMAT)) {
                            typeSet.add(convertStringFormat(location, node.get(FORMAT)));
                        } else if (node.has(CONTENT_ENCODING)) {
                            typeSet.add(convertStringEncoding(location, node.get(CONTENT_ENCODING)));
                        } else {
                            typeSet.add(Types.STRING);
                        }
                        break;
                    case TYPE_NUMBER:
                        typeSet.add(Types.BIG_DEC);
                        break;
                    case TYPE_INTEGER:
                        // use BigDecimal for easier interoperability
                        // without affecting the correctness of the result
                        typeSet.add(Types.BIG_DEC);
                        break;
                    case TYPE_OBJECT:
                        typeSet.add(convertObject(location, node, root));
                        break;
                    case TYPE_ARRAY:
                        typeSet.add(convertArray(location, node, root));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unsupported type '" + node.get(TYPE).asText() + "' in node: " + location);
                }
            }
        }

        // simple interpretation of ONE_OF for supporting "object or null"
        if (node.has(ONE_OF) && node.get(ONE_OF).isArray()) {
            final TypeInformation<?>[] types = convertTypes(location + '/' + ONE_OF, node.get(ONE_OF), root);
            typeSet.addAll(Arrays.asList(types));
        }

        // validate no union types or extending
        if (node.has(ALL_OF) || node.has(ANY_OF) || node.has(NOT) || node.has(EXTENDS) || node.has(DISALLOW)) {
            throw new IllegalArgumentException(
                    "Union types are such as '" + ALL_OF + "', '" + ANY_OF + "' etc. " +
                            "and extending are not supported yet.");
        }

        // only a type (with null) is supported yet
        final List<TypeInformation<?>> types = new ArrayList<>(typeSet);
        if (types.size() == 0) {
            throw new IllegalArgumentException("No type could be found in node:" + location);
        } else if (types.size() > 2 || (types.size() == 2 && !types.contains(Types.VOID))) {
            throw new IllegalArgumentException(
                    "Union types with more than just a null type are not supported yet.");
        }

        // return the first non-void type or void
        if (types.size() == 2 && types.get(0) == Types.VOID) {
            return types.get(1);
        } else {
            return types.get(0);
        }
    }

    private static TypeInformation<Row> convertObject(String location, JsonNode node, JsonNode root) {
        // validate properties
        if (!node.has(PROPERTIES)) {
            return Types.ROW();
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException(
                    "Invalid '" + PROPERTIES + "' property for object type in node: " + location);
        }
        final JsonNode props = node.get(PROPERTIES);
        final String[] names = new String[props.size()];
        final TypeInformation<?>[] types = new TypeInformation[props.size()];

        final Iterator<Map.Entry<String, JsonNode>> fieldIter = props.fields();
        int i = 0;
        while (fieldIter.hasNext()) {
            final Map.Entry<String, JsonNode> subNode = fieldIter.next();

            // set field name
            names[i] = subNode.getKey();

            // set type
            types[i] = convertType(location + '/' + subNode.getKey(), subNode.getValue(), root);

            i++;
        }

        // validate that object does not contain additional properties
        if (node.has(ADDITIONAL_PROPERTIES) && node.get(ADDITIONAL_PROPERTIES).isBoolean() &&
                node.get(ADDITIONAL_PROPERTIES).asBoolean()) {
            throw new IllegalArgumentException(
                    "An object must not allow additional properties in node: " + location);
        }

        return Types.ROW_NAMED(names, types);
    }

    private static TypeInformation<?> convertArray(String location, JsonNode node, JsonNode root) {
        // validate items
        if (!node.has(ITEMS)) {
            throw new IllegalArgumentException(
                    "Arrays must specify an '" + ITEMS + "' property in node: " + location);
        }
        final JsonNode items = node.get(ITEMS);

        // list (translated to object array)
        if (items.isObject()) {
            final TypeInformation<?> elementType = convertType(
                    location + '/' + ITEMS,
                    items,
                    root);
            // result type might either be ObjectArrayTypeInfo or BasicArrayTypeInfo for Strings
            return Types.OBJECT_ARRAY(elementType);
        }
        // tuple (translated to row)
        else if (items.isArray()) {
            final TypeInformation<?>[] types = convertTypes(location + '/' + ITEMS, items, root);

            // validate that array does not contain additional items
            if (node.has(ADDITIONAL_ITEMS) && node.get(ADDITIONAL_ITEMS).isBoolean() &&
                    node.get(ADDITIONAL_ITEMS).asBoolean()) {
                throw new IllegalArgumentException(
                        "An array tuple must not allow additional items in node: " + location);
            }

            return Types.ROW(types);
        }
        throw new IllegalArgumentException(
                "Invalid type for '" + ITEMS + "' property in node: " + location);
    }

    private static TypeInformation<?> convertStringFormat(String location, JsonNode node) {
        if (!node.isTextual()) {
            throw new IllegalArgumentException("Invalid '" + FORMAT + "' property in node: " + location);
        }

        switch (node.asText()) {
            case FORMAT_DATE:
                return Types.SQL_DATE;
            case FORMAT_TIME:
                return Types.SQL_TIME;
            case FORMAT_DATE_TIME:
                return Types.SQL_TIMESTAMP;
            default:
                return Types.STRING; // unlikely that we will support other formats in the future
        }
    }

    private static TypeInformation<?> convertStringEncoding(String location, JsonNode node) {
        if (!node.isTextual()) {
            throw new IllegalArgumentException("Invalid '" + CONTENT_ENCODING + "' property in node: " + location);
        }

        // "If the instance value is a string, this property defines that the string SHOULD
        // be interpreted as binary data and decoded using the encoding named by this property."

        switch (node.asText()) {
            case CONTENT_ENCODING_BASE64:
                return Types.PRIMITIVE_ARRAY(Types.BYTE);
            default:
                // we fail hard here:
                // this gives us the chance to support more encodings in the future without problems
                // of backwards compatibility
                throw new IllegalArgumentException("Invalid encoding '" + node.asText() + "' in node: " + location);
        }
    }

    private static TypeInformation<?>[] convertTypes(String location, JsonNode arrayNode, JsonNode root) {
        final TypeInformation<?>[] types = new TypeInformation[arrayNode.size()];
        final Iterator<JsonNode> elements = arrayNode.elements();
        int i = 0;
        while (elements.hasNext()) {
            final TypeInformation<?> elementType = convertType(
                    location + '[' + i + ']',
                    elements.next(),
                    root);
            types[i] = elementType;
            i += 1;
        }
        return types;
    }
}
