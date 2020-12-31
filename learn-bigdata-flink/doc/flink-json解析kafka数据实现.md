### flink-json解析kafka数据实现

#### 1.需要解决的问题

1.reader配置文件解析

2.jsonSchema校验

3.按照reader配置对一些信息进行封装，例如：别名，拉平array字段记录，path，类型等等

4.是否需要提供API，根据path获取指定数据

5.如何实现解析，需要将所有字段拉平



#### 2.flink-json如何实现根据schema解析



##### 1.flink是如何根据JsonSchema解析json

解析示例

```java
    public static void main(String[] args) throws IOException {
        JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(SCHEMA1).build();
        Row row = deserializationSchema.deserialize(INPUT1.getBytes("UTF-8"));
        System.out.println(row);
        RowTypeInfo producedType = ((RowTypeInfo) deserializationSchema.getProducedType());
        String[] fieldNames = producedType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            System.out.println(fieldNames[i]);
        }
    }
```

上面的例子展示了通过flink-json解析json中不同类型数据的示例。

解析步骤主要有两步：

1.通过Builder类创建JsonRowDeserializationSchema，在构建时需传入JsonSchema参数。

2.通过JsonRowDeserializationSchema对象反序列化json字符串，得到一个Row对象。



##### 2.源码分析

解析的整理流程如下图所示

![flink-json解析为row流程](F:\src\tuling\project-all\learn-bigdata-flink\doc\flink-json解析为row流程.png)

先分析第一句

```java
JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(SCHEMA1).build();
```



#### 思路

分析一下现在是怎么创建RowConverter的，代码如下：

```java
private DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<DeserializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(this::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
	}
```

主要步骤有两步：

1.根据row的field类型创建每个field的runtimeConverter

这一步比较简单，就是根据field类型再调用createConverter方法，说明创建converter的过程是一个递归创建的过程

2.调用assembleRowConverter方法并返回结果

下面具体看一下代码：

```java
private DeserializationRuntimeConverter assembleRowConverter(
		String[] fieldNames,
		List<DeserializationRuntimeConverter> fieldConverters) {
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

这个方法里面创建了一个converter，而这个converter的作用就是创建Row对象。

它的步骤如下：

1.遍历fieldNames

2.根据fieldName，field，filedConverter，mapper这几个参数，拿到field的值

3.将field的值赋值给Row

4.返回Row对象



JsonRowSchemaConverter源码分析