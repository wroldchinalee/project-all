代码如下：

```java
// 创建一个内存source，将json传进去，json的colName是message
        MemSourceBatchOp inOp = new MemSourceBatchOp(jsonArr, "message");
        // 将source与数据处理操作链接
        List<Row> message = inOp.link(new JsonValueBatchOp()
                .setSkipFailed(true)
                // 通过message拿到json
                .setSelectedCol("message")
                // 输出的字段
                .setOutputCols(new String[]{"db", "table", "op", "data_id", "data_name"})
                // 上面输出字段在json中的路径
                .setJsonPath(new String[]{"$.db", "$.table", "$.op", "$.datas.id", "$.datas.name"})).collect();
        // 打印输出
        for (Row row : message) {
            System.out.println(row);
        }
```

![JsonValueBatchOp](E:\source_code\project-all\learn-bigdata-alink\src\doc\JsonValueBatchOp.png)

```java
// 创建JsonValueBatchOp
public JsonValueBatchOp(Params param) {
		super(JsonPathMapper::new, param);
}
```

JsonPathMapper构造方法

```java
public JsonPathMapper(TableSchema dataSchema, Params params) {
		......
}
```



MemSourceBatchOp.link方法代码如下：

```java
// next参数就是JsonValueBatchOp对象
// next调用的linkFrom方法来自于FlatMapBatchOp
public <B extends BatchOperator <?>> B link(B next) {
		next.linkFrom(this);
		return next;
	}
```

FlatMapBatchOp类的linkFrom方法如下：

```java
    public T linkFrom(BatchOperator<?>... inputs) {
        // 拿到第一个input，也就是MemSourceBatchOp
        BatchOperator<?> in = checkAndGetFirst(inputs);

        try {
            // 创建flatMapper
            //mapperBuilder是一个BiFunction接口，apply方法是给定两个参数，然后返回一个值，在这里里面其实就是JsonPathMapper::new，new JsonPathMapper(TableSchema dataSchema, Params params),创建一个JsonPathMapper对象，而JsonPathMapper对象就是FlatMapper的子类
            // 从input里面获取输入数据的schema，并且传入参数
            FlatMapper flatmapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
            // 对input执行flatmapper的方法，实际就是JsonPathMapper里面的flatMap方法
            DataSet<Row> resultRows = in.getDataSet().flatMap(new FlatMapperAdapter(flatmapper));
            // 获得输出的schema
            TableSchema resultSchema = flatmapper.getOutputSchema();
            this.setOutput(resultRows, resultSchema);
            return (T) this;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
```



```java
// 这一句里面，flatMap传入了一个FlatMapFunction，实际类型为FlatMapperAdapter
DataSet<Row> resultRows = in.getDataSet().flatMap(new FlatMapperAdapter(flatmapper));
```

继承关系如下图：

![FlatMapperAdapter](E:\source_code\project-all\learn-bigdata-alink\src\doc\FlatMapperAdapter.png)



```java
// flatMap函数的输入和输出类型都为ROW类型
public class FlatMapperAdapter extends RichFlatMapFunction<Row, Row> implements Serializable {

    private final FlatMapper mapper;

    public FlatMapperAdapter(FlatMapper mapper) {
        this.mapper = mapper;
    }
	省略部分代码。。。
    // 重写了flatMap方法，输入为一条条的row，输出为row的集合
    // 实际调用的是上面的JsonPathMapper对象
    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        this.mapper.flatMap(value, out);
    }
}
```

下面看一下最主要的类：JsonPathMapper

```java
public class JsonPathMapper extends FlatMapper {

	private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	private String[] jsonPaths;
	private boolean skipFailed;
	private String[] outputColNames;
	private OutputColsHelper outputColsHelper;
	private int idx;

	public JsonPathMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		// 选择的列
		String selectedColName = this.params.get(JsonValueParams.SELECTED_COL);
		// 根据选择列找到它的index
		this.idx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), selectedColName);
		// 输出的列
		outputColNames = params.get(JsonValueParams.OUTPUT_COLS);
		// 输出的列在输入的json中的路径数组，每个列对应一个路径
		jsonPaths = params.get(JsonValueParams.JSON_PATHS);

		skipFailed = params.get(JsonValueParams.SKIP_FAILED);
		for (int i = 0; i < outputColNames.length; ++i) {
			outputColNames[i] = outputColNames[i].trim();
		}

		if (jsonPaths.length != outputColNames.length) {
			throw new IllegalArgumentException(
				"jsonPath and outputColName mismatch: " + jsonPaths.length + " vs " + outputColNames.length);
		}

		// 每个字段的类型信息
		int numField = jsonPaths.length;
		TypeInformation[] types = new TypeInformation[numField];
		for (int i = 0; i < numField; i++) {
			types[i] = Types.STRING;
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColNames, types,
			this.params.get(HasReservedCols.RESERVED_COLS));
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		Row res = new Row(jsonPaths.length);

		// 获取row中的select列
		String json = (String) row.getField(idx);
		// 如果json为null
		if (StringUtils.isNullOrWhitespaceOnly(json)) {
			if (skipFailed) {
				output.collect(outputColsHelper.getResultRow(row, res));
			} else {
				throw new RuntimeException("empty json string");
			}
		} else {
			// 遍历jsonPath中的路径，根据路径获得对应的value，封装成row
			for (int i = 0; i < jsonPaths.length; i++) {
				Object obj = null;
				try {
					obj = JsonPath.read(json, jsonPaths[i]);
					if (!(obj instanceof String)) {
						obj = gson.toJson(obj);
					}
					res.setField(i, obj);
				} catch (Exception ex) {
					if (skipFailed) {
						res.setField(i, null);
					} else {
						throw new RuntimeException("Fail to getVector json path: " + ex);
					}
				}
			}
			// 发送到下游
			output.collect(outputColsHelper.getResultRow(row, res));
		}
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}
}
```

```java

```

