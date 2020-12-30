### 读取Kafka数据方案

#### 1.需求分析

现在以来源为数据库，比较常见的两种kafka json格式来说明需求达到的效果。

第一种数据格式如下：

```json
{
	"schema": "dcbsdb",
	"table": "xy",
	"eventType": "update",
	"before": {
		"x": "1",
		"y": "3"
	},
	"after": {
		"x": "4",
		"y": "5"
	}
}
```

数据中的几个字段的含义如下：数据库名，表名，操作类型，更新前的数据，更新后的数据。

一条数据中对应的是数据库中的一行数据。

第二种数据格式如下：

```json
{
	"__db": "dcbsdb",
	"__table": "xy",
	"__op": "U",
	"data": [{
		"before_x": "1",
		"before_y": "3",
        "x":"3",
        "y":"3"
	}, {
        "before_x":"3",
        "before_y":"4",
		"x": "8",
		"y": "10",
	}]
}
```

数据中的几个字段的含义如下：数据库名，表名，操作类型，数据集合。其中数据集合中包含了两行数据，每行数据中包含了更新前的数据和更新后的数据。

**从这两个不同格式的解析需求来看，我们可以发现下列问题：**

**1.一个字段在两种不同的数据格式中字段名不一致**

**2.一个字段的字段值在两种不同的数据格式中值不一致**

**3.表示数据库中一行数据的方式不一致**



再来看一下我们希望数据输出要达到的效果，

最终输出的数据格式为Row类型，那么如果在不过滤任何字段的情况下，

第一种格式的输出如下：

```java
"dcbsdb","xy","update","1","3","4","5"
```

第二种格式的输出如下：

```java
"dcbsdb","xy","U","1","3","3","3"
"dcbsdb","xy","U","3","4","8","10"
```

从两种输出格式来看，需要对某些字段进行拉平的操作。

第一种格式需要拉平的字段为before和after字段，这两个字段的类型可以看做是object类型，

将object中的字段拉平，依次填充到Row中。

第二种格式需要拉平的字段为data字段，data字段的类型是array类型，array里的数组元素的类型

为object类型，和第一种格式不同的地方在于因为data为array类型，所以它里面可能会包含多行

数据，需要将多行数据提取出来，再遍历每行与其他字段拼接形成多个Row。



#### 2.flink-json实现分析

flink是如何根据JsonSchema解析json

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

按照上面代码输出结果：

第一种格式为

```java
dcbsdb,xy,update,1,3,4,5
schema,table,eventType,before,after
```

输出的字段为schema,table,eventType,before,after

但是我们想要的输出字段为schema,table,eventType,x,y,x,y

第二种格式为

```java
dcbsdb,xy,U,[1,3,3,3, 3,4,8,10]
__db,__table,__op,data
```

从两种格式的输出结果来看

输出的字段为\_\_db,_\_table,__op,data

但是我们想要的输出字段为\_\_db,_\_table,__op,before_x,before_y,x,y

与我们需要的输出是有一些区别，我们需要做一些额外的处理。



#### 3.解析实现

为了减少代码实现难度，对JsonSchama做一些硬性规定，规定如下：

**字段拉平限制**

1)拉平的字段必须是第一层

2)拉平字段的的类型只能是数组或者对象

如果是数组，数组中元素是object类型，会将每个字段提取拉平

数组中元素是其他类型，不做任何处理

如果是object类型，直接将每个字段提取拉平

3)拉平的字段最多支持两个

如果配置了两个的情况，两个字段中属性的名称和个数需要一致



**一个字段在两种不同的数据格式中字段名不一致**

对于一些固定的字段，在不同的格式中，名称不一致，需要在管理台指定它的别名，这个别名是固定的，

提供下拉框来选择。

目前有4个固定字段如下：

| 字段     | 别名       |
| -------- | ---------- |
| 数据库名 | __db       |
| 表名     | __table    |
| 操作类型 | __op       |
| 事件时间 | __event_ts |



**一个字段的字段值在两种不同的数据格式中值不一致**

对于一些类型和固定字段的值，统一进行转换处理。

| 字段或类型 | 固定值                                     |
| ---------- | ------------------------------------------ |
| __op       | 插入:I,更新:U,删除:D                       |
| __event_ts | 统一转换为string类型，格式为long样式时间戳 |



#### 4.管理台配置

管理台中数据类型与flink在json-schema是一致的

![image-20201228163745478](E:\source_code\project-all\learn-bigdata-flink\doc\image-20201228163745478.png)

配置目前包含以下配置项：

| 配置项   | 含义                                                         |
| -------- | ------------------------------------------------------------ |
| path     | 表示配置的字段在schema的路径，子路径用“.”表示                |
| column   | json中的字段名称                                             |
| alias    | 别名，需要具有唯一性                                         |
| type     | 字段类型，array和表示时间的string类型特殊，array需要在typeInfo中额外指定array里的元素类型，而时间字符串支持三种格式 |
| typeInfo | 表示一些类型的额外信息，如array，时间字符串                  |
| flat     | 字段是否可以拉平                                             |

时间字符串目前支持三种格式，如下：

| 类型   | 格式      | 配置方式                                           | 显示                |
| ------ | --------- | -------------------------------------------------- | ------------------- |
| string | date      | date: { type: 'string', format: 'date' }           | 1990-10-14          |
| string | time      | time: { type: 'string', format: 'time' }           | 12:12:43            |
| string | date-time | timestamp: { type: 'string', format: 'date-time' } | 1990-10-14 12:12:43 |



第一种数据格式配置：

| path      | oriname   | rename     | type   | typeInfo | flat |
| --------- | --------- | ---------- | ------ | -------- | ---- |
| schema    | schema    | __db       | string |          |      |
| table     | table     | __table    | string |          |      |
| eventType | eventType | __op       | string |          |      |
| timestamp | timestamp | __event_ts | number |          |      |
| before    | before    | before     | object |          | √    |
| before.x  | x         | before_x   | string |          |      |
| before.y  | y         | before_y   | string |          |      |
| after     | after     | after      | object |          | √    |
| after.x   | x         | x          | string |          |      |
| after.y   | y         | y          | string |          |      |

转换为json的shcema为：

```json
“schema”:{
	"type": "object",
	"properties": {
		"schema": {
			"type": "string"
		},
		"table": {
			"type": "string"
		},
		"eventType": {
			"type": "string"
		},
		"timestamp": {
			"type": "number"
		},
		"before": {
			"type": "object",
			"properties": {
				"x": {
					"type": "string"
				},
				"y": {
					"type": "string"
				}
			}
		},
		"after": {
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
```



第二种数据格式配置：

| path          | oriname      | rename     | type   | typeInfo         | flat |
| ------------- | ------------ | ---------- | ------ | ---------------- | ---- |
| __db          | __db         | __db       | string |                  |      |
| __table       | __table      | __table    | string |                  |      |
| __op          | __op         | __op       | string |                  |      |
| __binlogTime  | __binlogTime | __event_ts | string | format:date-time |      |
| data          | data         | data       | array  | itemType:object  | √    |
| data.before_x | before_x     | before_x   | string |                  |      |
| data.before_y | before_y     | before_y   | string |                  |      |
| data.x        | x            | x          | string |                  |      |
| data.y        | y            | y          | string |                  |      |

转换为json的shcema为：

```json
{
	"type": "object",
	"properties": {
		"__db": {
			"type": "string"
		},
		"__table": {
			"type": "string"
		},
		"__op": {
			"type": "string"
		},
		"__binlogTime": {
			"type": "string",
			"format": "date-time"
		},
		"data": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"before_x": {
						"type": "string"
					},
					"before_y": {
						"type": "string"
					},
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



#### 5.配置文件格式

第一种格式kafka reader配置如下

```json
{
	"reader": {
		"parameter": {
			"topic": "kafka10",
			"groupId": "default",
			"codec": "json",
			"failOnMissingField": false,
			"consumerSettings": {
				"zookeeper.connect": "0.0.0.1:2182/kafka",
				"bootstrap.servers": "0.0.0.1:9092",
				"auto.commit.interval.ms": "1000",
				"auto.offset.reset": "earliest"
			},
			"schema": {
				"type": "object",
				"properties": {
					"schema": {
						"type": "string"
					},
					"table": {
						"type": "string"
					},
					"eventType": {
						"type": "string"
					},
					"timestamp": {
						"type": "number"
					},
					"before": {
						"type": "object",
						"properties": {
							"x": {
								"type": "string"
							},
							"y": {
								"type": "string"
							}
						}
					},
					"after": {
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
			},
			"column": [{
					"name": "__db",
					"type": "string",
					"oriname": "schema",
					"path": "schema"
				},
				{
					"name": "__table",
					"type": "string",
					"oriname": "table",
					"path": "table"
				},
				{
					"name": "__op",
					"type": "string",
					"oriname": "eventType",
					"path": "eventType"
				},
				{
					"name": "__event_ts",
					"type": "long",
					"oriname": "timestamp",
					"path": "timestamp"
				},
				{
					"name": "before_x",
					"type": "string",
					"oriname": "x",
					"path": "before.x"
				},
				{
					"name": "before_y",
					"type": "string",
					"oriname": "y",
					"path": "before.y"
				},
				{
					"name": "x",
					"type": "string",
					"oriname": "x",
					"path": "after.x"
				},
				{
					"name": "y",
					"type": "string",
					"oriname": "y",
					"path": "after.y"
				}
			],
			"flat":["before","after"]
		}
		"name": "kafka10reader"
	}
}
```

第二种格式kafka reader配置如下：

```
{
	"reader": {
		"parameter": {
			"topic": "kafka10",
			"groupId": "default",
			"codec": "json",
			"failOnMissingField": false,
			"consumerSettings": {
				"zookeeper.connect": "0.0.0.1:2182/kafka",
				"bootstrap.servers": "0.0.0.1:9092",
				"auto.commit.interval.ms": "1000",
				"auto.offset.reset": "earliest"
			},
			"schema": {
				"type": "object",
				"properties": {
					"__db": {
						"type": "string"
					},
					"__table": {
						"type": "string"
					},
					"__op": {
						"type": "string"
					},
					"__binlogTime": {
						"type": "string",
						"format": "date-time"
					},
					"data": {
						"type": "array",
						"items": {
							"type": "object",
							"properties": {
								"before_x": {
									"type": "string"
								},
								"before_y": {
									"type": "string"
								},
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
			},
			"column": [{
					"name": "__db",
					"type": "string",
					"oriname": "__db",
					"path": "__db"
				},
				{
					"name": "__table",
					"type": "string",
					"oriname": "__table",
					"path": "__table"
				},
				{
					"name": "__op",
					"type": "string",
					"oriname": "__op",
					"path": "__op"
				},
				{
					"name": "__event_ts",
					"type": "string",
					"oriname": "__binlogTime",
					"path": "__binlogTime"
				},
				{
					"name": "before_x",
					"type": "string",
					"oriname": "before_x",
					"path": "data.before_x"
				},
				{
					"name": "before_y",
					"type": "string",
					"oriname": "before_y",
					"path": "data.before_y"
				},
				{
					"name": "x",
					"type": "string",
					"oriname": "x",
					"path": "data.x"
				},
				{
					"name": "y",
					"type": "string",
					"oriname": "y",
					"path": "data.y"
				}
			],
			"flat":["data"]
		}
		"name": "kafka10reader"
	}
}
```

