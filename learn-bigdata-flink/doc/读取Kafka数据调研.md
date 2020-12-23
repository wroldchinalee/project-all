### 读取Kafka数据调研



#### Flink读取Json

##### 测试代码：

```java
public class FlinkKafkaSqlMain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(
                // kafka连接配置
                new Kafka().version("0.10").topic("test2")
                .startFromEarliest().property("zookeeper.connect", "192.168.233.130:2181/kafka")
                .property("bootstrap.servers", "192.168.233.130:9092"))
                // 表schema定义
                .withSchema(getSchema())
                // kafka读取格式定义，这里读取json格式，并且在没有字段时抛异常
                .withFormat(new Json().failOnMissingField(true)
                        // json schema，可以自己指定，也可以通过表schema派生
                        .jsonSchema(JSON_SCHEMA_PARSE)).inAppendMode()
                .registerTableSource("amvcha");


//        Table filterTable = tableEnv.sqlQuery("select __db,data[1].x,data[1].y,data[2].x,data[2].y,data[3].x,data[3].y from amvcha");
        Table filterTable = tableEnv.sqlQuery("select * from amvcha");
        DataStream<Row> resultStream = tableEnv.toAppendStream(filterTable, TypeInformation.of(Row.class));
        resultStream.print();
        env.execute("FlinkKafkaSqlMain");
    }
```



##### JsonSchema

```json
{
	"type": "object",
	"properties": {
		"__db": {
			"type": "string"
		},
		"data": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"x": {
						"type": "string"
					},
					"y": {
						"type": "string"
					}
				}
			}
		}
	}
}
```

##### 输入

```json
{"__db":"dcbsdb","data":[{"x":"1","y":"3"},{"x":"8","y":"10"},{"x":"9","y":"10"}]}
{"__db":"dcbsdb","data":[{"x":"1","y":"3"},{"x":"4","y":"7"},{"x":"3","y":"6"}]}
{"__db":"dcbsdb","data":[{"x":"5","y":"7"},{"x":"3","y":"5"},{"x":"6","y":"1"}]}
{"__db":"dcbsdb","data":[{"x":"3","y":"9"},{"x":"2","y":"4"},{"x":"9","y":"1"}]}
{"__db":"dcbsdb","data":[{"x":"1","y":"1"},{"x":"9","y":"1"},{"x":"3","y":"4"}]}
{"__db":"dcbsdb","data":[{"x":"2","y":"9"},{"x":"6","y":"5"},{"x":"6","y":"1"}]}
{"__db":"dcbsdb","data":[{"x":"6","y":"3"},{"x":"7","y":"7"},{"x":"6","y":"8"}]}
{"__db":"dcbsdb","data":[{"x":"1","y":"8"},{"x":"3","y":"4"},{"x":"9","y":"7"}]}
{"__db":"dcbsdb","data":[{"x":"1","y":"5"},{"x":"7","y":"1"},{"x":"10","y":"6"}]}
{"__db":"dcbsdb","data":[{"x":"6","y":"10"},{"x":"9","y":"5"},{"x":"8","y":"6"}]}
```

##### 期望效果

> // 以第一条数据为例
> **dcbsdb,1,3**
> **dcbsdb,8,10**
> **dcbsdb,9,10**

##### 输出

*SQL:select __db,data from amvcha*;

这种情况，会将data数组里面的多条数据拉平，变为一个object数组。

> **dcbsdb,[1,3, 8,10, 9,10]**
> **dcbsdb,[1,3, 4,7, 3,6]**
> **dcbsdb,[5,7, 3,5, 6,1]**
> **dcbsdb,[3,9, 2,4, 9,1]**
> **dcbsdb,[1,1, 9,1, 3,4]**
> **dcbsdb,[2,9, 6,5, 6,1]**
> **dcbsdb,[6,3, 7,7, 6,8]**
> **dcbsdb,[1,8, 3,4, 9,7]**
> **dcbsdb,[1,5, 7,1, 10,6]**
> **dcbsdb,[6,10, 9,5, 8,6]**

*SQL:select __db,data[1].x,data[1].y,data[2].x,data[2].y,data[3].x,data[3].y from amvcha*;

这种情况，会将data数组里面的多条数据拉平，和前面的字段整体形成一个row。

> **dcbsdb,1,3,8,10,9,10**
> **dcbsdb,1,3,4,7,3,6**
> **dcbsdb,5,7,3,5,6,1**
> **dcbsdb,3,9,2,4,9,1**
> **dcbsdb,1,1,9,1,3,4**
> **dcbsdb,2,9,6,5,6,1**
> **dcbsdb,6,3,7,7,6,8**
> **dcbsdb,1,8,3,4,9,7**
> **dcbsdb,1,5,7,1,10,6**
> **dcbsdb,6,10,9,5,8,6**

*SQL:select __db, x1,y1 from amvcha, LATERAL TABLE(myflat(data)) as T(x1,y1)*;

这种情况，先将data用自定义表函数变为多行，再进行拼接。

> **dcbsdb,1,3**
> **dcbsdb,8,10**
> **dcbsdb,9,10**
> **dcbsdb,1,3**
> **dcbsdb,4,7**
> **dcbsdb,3,6**
> **dcbsdb,5,7**
> **dcbsdb,3,5**
> **dcbsdb,6,1**
> **dcbsdb,3,9**
> **dcbsdb,2,4**
> **dcbsdb,9,1**
> **dcbsdb,1,1**
> **dcbsdb,9,1**
> **dcbsdb,3,4**
> **dcbsdb,2,9**
> **dcbsdb,6,5**
> **dcbsdb,6,1**
> **dcbsdb,6,3**
> **dcbsdb,7,7**
> **dcbsdb,6,8**
> **dcbsdb,1,8**
> **dcbsdb,3,4**
> **dcbsdb,9,7**
> **dcbsdb,1,5**
> **dcbsdb,7,1**
> **dcbsdb,10,6**
> **dcbsdb,6,10**
> **dcbsdb,9,5**
> **dcbsdb,8,6**

第三种SQL使用了自定义的UDTF函数，将一行转换成了多行，基本满足了输出格式。

但是还是存在以下问题：

1.只通过flink解析json只能通过shcema获取指定的字段，但是在处理多条数据的情况需要引额外入自定义函数，将一行数据转为多行，并且指定对应的数据字段。

2.管理台拼接json schema复杂，存在多层嵌套等情况

3.在reader中就需要引入flink sql api，如果不使用sql解析，只通过flink本身的方式还需要调研是否可以实现

4.目前调研结果都是基于flink自带的kafka connect，与准实时目前百信的实现区别较大



##### 其他解析案例

这个解析是直接使用JsonRowDeserializationSchema解析的，解析了大多数数据类型的情况。

示例代码：

```java
public void test() throws Exception {
		final BigDecimal id = BigDecimal.valueOf(1238123899121L);
		final String name = "asdlkjasjkdla998y1122";
		final byte[] bytes = new byte[1024];
		ThreadLocalRandom.current().nextBytes(bytes);
		final BigDecimal[] numbers = new BigDecimal[] {
				BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3)};
		final String[] strings = new String[] {"one", "two", "three"};

		final ObjectMapper objectMapper = new ObjectMapper();

		// Root
		ObjectNode root = objectMapper.createObjectNode();
		root.put("id", id.longValue());
		root.putNull("idOrNull");
		root.put("name", name);
		root.put("date", "1990-10-14");
		root.put("time", "12:12:43Z");
		root.put("timestamp", "1990-10-14T12:12:43Z");
		root.put("bytes", bytes);
		root.putArray("numbers").add(1).add(2).add(3);
		root.putArray("strings").add("one").add("two").add("three");
		root.putObject("nested").put("booleanField", true).put("decimalField", 12);

		final byte[] serializedJson = objectMapper.writeValueAsBytes(root);

		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema.Builder(
				"{" +
						"    type: 'object'," +
						"    properties: {" +
						"         id: { type: 'integer' }," +
						"         idOrNull: { type: ['integer', 'null'] }," +
						"         name: { type: 'string' }," +
						"         date: { type: 'string', format: 'date' }," +
						"         time: { type: 'string', format: 'time' }," +
						"         timestamp: { type: 'string', format: 'date-time' }," +
						"         bytes: { type: 'string', contentEncoding: 'base64' }," +
						"         numbers: { type: 'array', items: { type: 'integer' } }," +
						"         strings: { type: 'array', items: { type: 'string' } }," +
						"         nested: { " +
						"             type: 'object'," +
						"             properties: { " +
						"                 booleanField: { type: 'boolean' }," +
						"                 decimalField: { type: 'number' }" +
						"             }" +
						"         }" +
						"    }" +
						"}").build();

		Row row = deserializationSchema.deserialize(serializedJson);
		System.out.println(row);

	}
```

