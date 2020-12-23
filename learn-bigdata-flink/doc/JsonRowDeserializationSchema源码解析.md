JsonRowDeserializationSchema源码解析

类的继承结构如下：

![image-20201223102847958](E:\source_code\project-all\learn-bigdata-flink\doc\image-20201223102847958.png)



先来分析一下成员变量

```java
/** Type information describing the result type. */
// 用来描述json的数据类型
private final RowTypeInfo typeInfo;
// 没有指定字段时是否抛异常
private boolean failOnMissingField;

/** Object mapper for parsing the JSON. */
private final ObjectMapper objectMapper = new ObjectMapper();
// 根据typeInfo创建的反序列化器
private DeserializationRuntimeConverter runtimeConverter;
```

如何创建JsonRowDeserializationSchema对象

创建该对象一般是通过Builder来创建，代码如下：

```java
JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(jsonSchema).build()
```

Builder类的代码如下：

```java
	public static class Builder {
		// 根据jsonSchema映射出来的类型信息
		private final RowTypeInfo typeInfo;
		private boolean failOnMissingField = false;

		/**
		 * Creates a JSON deserialization schema for the given type information.
		 *
		 * @param typeInfo Type information describing the result type. The field names of {@link Row}
		 *                 are used to parse the JSON properties.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
			this.typeInfo = (RowTypeInfo) typeInfo;
		}

		/**
		 * Creates a JSON deserialization schema for the given JSON schema.
		 *
		 * @param jsonSchema JSON schema describing the result type
		 *
		 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
		 */
        // 调用JsonRowSchemaConverter.convert方法，将jsonSchema转换成TypeInformation
		public Builder(String jsonSchema) {
			this(JsonRowSchemaConverter.convert(checkNotNull(jsonSchema)));
		}

		/**
		 * Configures schema to fail if a JSON field is missing.
		 *
		 * <p>By default, a missing field is ignored and the field is set to null.
		 */
		public Builder failOnMissingField() {
			this.failOnMissingField = true;
			return this;
		}

        // 创建JsonRowDeserializationSchema，将类型信息传进去
		public JsonRowDeserializationSchema build() {
			return new JsonRowDeserializationSchema(typeInfo, failOnMissingField);
		}
	}
```

接下来看一下JsonRowDeserializationSchema的构造函数

```java
	private JsonRowDeserializationSchema(
			TypeInformation<Row> typeInfo,
			boolean failOnMissingField) {
        // 校验
		checkNotNull(typeInfo, "Type information");
		checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.failOnMissingField = failOnMissingField;
        // 创建converter，就是通过这个来解析json
		this.runtimeConverter = createConverter(this.typeInfo);
	}
```

再来看一下createConverter方法

这个方法主要是通过typeInfo创建一个converter，和类型结构是一致的，如果类型是嵌套的，那么converter也是

嵌套的。

```java
private DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		DeserializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() ->
				createContainerConverter(typeInfo)
					.orElseGet(() -> createFallbackConverter(typeInfo.getTypeClass())));
		return wrapIntoNullableConverter(baseConverter);
}
```

我们一步步来看

1.第一个方法是createConverterForSimpleType，它的返回值是一个Optional，泛型就是DeserializationRuntimeConverter。

2.接下来是调用Optional的orElseGet方法，如果Optional里面的value不为null，那么baseConverter就是这个value了，如Optional里的value为null，继续第3步

3.调用createContainerConverter方法创建converter，如果typeInfo是下列4中类型之一的：

Row，ObjectArray，BasicArray，PrimitiveByteArray就创建converter，返回Optional。

4.如果Optional里的value不为null，baseConverter为value值，否则，就继续调用createFallbackConverter方法。

5.调用createFallbackConverter方法，说明typeInfo的类型是一个POJO类。

6.通过wrapIntoNullableConverter再封装一下baseConverter，如果传入的jsonnode为null，直接返回null。



Converter的定义如下：

```java
@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(ObjectMapper mapper, JsonNode jsonNode);
	}
```

只是一个借口，定义了converter方法。参数时mapper和jsonNode。

它的具体是通过匿名内部类。具体代码在createConverterForSimpleType，createContainerConverter和createFallbackConverter方法里。

createConverterForSimpleType的代码如下：

```java
private Optional<DeserializationRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of((mapper, jsonNode) -> null);
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asBoolean());
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asText());
		} else if (simpleTypeInfo == Types.INT) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asInt());
		} else if (simpleTypeInfo == Types.LONG) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asLong());
		} else if (simpleTypeInfo == Types.DOUBLE) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asDouble());
		} else if (simpleTypeInfo == Types.FLOAT) {
			return Optional.of((mapper, jsonNode) -> Float.parseFloat(jsonNode.asText().trim()));
		} else if (simpleTypeInfo == Types.SHORT) {
			return Optional.of((mapper, jsonNode) -> Short.parseShort(jsonNode.asText().trim()));
		} else if (simpleTypeInfo == Types.BYTE) {
			return Optional.of((mapper, jsonNode) -> Byte.parseByte(jsonNode.asText().trim()));
		} else if (simpleTypeInfo == Types.BIG_DEC) {
			return Optional.of((mapper, jsonNode) -> jsonNode.decimalValue());
		} else if (simpleTypeInfo == Types.BIG_INT) {
			return Optional.of((mapper, jsonNode) -> jsonNode.bigIntegerValue());
		} else if (simpleTypeInfo == Types.SQL_DATE) {
			return Optional.of(createDateConverter());
		} else if (simpleTypeInfo == Types.SQL_TIME) {
			return Optional.of(createTimeConverter());
		} else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
			return Optional.of(createTimestampConverter());
		} else {
			return Optional.empty();
		}
	}
```

createContainerConverter的代码如下：

```java
	private Optional<DeserializationRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof RowTypeInfo) {
			return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (isPrimitiveByteArray(typeInfo)) {
			return Optional.of(createByteArrayConverter());
		} else {
			return Optional.empty();
		}
	}
```

createFallbackConverter的代码如下：

```
	private DeserializationRuntimeConverter createFallbackConverter(Class<?> valueType) {
		return (mapper, jsonNode) -> {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return mapper.treeToValue(jsonNode, valueType);
			} catch (JsonProcessingException e) {
				throw new WrappingRuntimeException(format("Could not convert node: %s", jsonNode), e);
			}
		};
	}
```

我们主要分析创建row类型的converter，代码如下：

```java
private DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
   List<DeserializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
      .map(this::createConverter)
      .collect(Collectors.toList());

   return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
}
```

通过代码可以看到，根据typeInfo里的fields，遍历fields创建不同的converter，得到与fields对应的fieldConverter。然后调用assembleRowConverter方法。

代码如下：

```java
private DeserializationRuntimeConverter assembleRowConverter(
   String[] fieldNames,
   List<DeserializationRuntimeConverter> fieldConverters) {
   // 装配converter
   return (mapper, jsonNode) -> {
      ObjectNode node = (ObjectNode) jsonNode;

      int arity = fieldNames.length;
      Row row = new Row(arity);
      for (int i = 0; i < arity; i++) {
         String fieldName = fieldNames[i];
         JsonNode field = node.get(fieldName);
         Object convertField = convertField(mapper, fieldConverters.get(i), fieldName, field);
         row.setField(i, convertField);
      }

      return row;
   };
}
```

这个方法里面实现了RowConverter，也就是通过fieldNames，fieldConverter这两个变量，将每个field的值从jsonNode中提取出来，并最终封装为Row类型。

其中convertField方法比较重要，它其实就是通过之前生成的fieldConverter执行converter方法。

fieldConverter的创建其实还是通过前面createConverterForSimpleType，createContainerConverter和createFallbackConverter这几个方法，如果是基本类型，返回的就是jsonNode的值。所以就能提取到json里面对应field的值了。

```java
private Object convertField(
		ObjectMapper mapper,
		DeserializationRuntimeConverter fieldConverter,
		String fieldName,
		JsonNode field) {
		if (field == null) {
			if (failOnMissingField) {
				throw new IllegalStateException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			return fieldConverter.convert(mapper, field);
		}
	}
```

到这里**this.runtimeConverter = createConverter(this.typeInfo);**这句代码就执行完毕了。





后面反序列化的工作就很简单了，方法是deserialize(byte[] message)

代码如下：

```java
@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return (Row) runtimeConverter.convert(objectMapper, root);
		} catch (Throwable t) {
			throw new IOException("Failed to deserialize JSON object.", t);
		}
	}
```

就是直接调用前面的rowConverter的convert方法，最后返回一个Row，得到结果。