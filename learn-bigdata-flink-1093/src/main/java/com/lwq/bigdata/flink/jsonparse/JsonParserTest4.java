package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/12/24
 * @description: JsonParserTest4
 **/
public class JsonParserTest4 {

    @Test
    public void testFlatMap() throws Exception {
        List<String> cols = Arrays.asList(/*"data", */"data2");
        byte[] bytes = getJsonString().getBytes("UTF-8");
        JsonParser jsonParser = new JsonParser.Builder(JsonSchemaHolder.JSON_SCHEMA6).build();
        RowTypeInfo producedType = (RowTypeInfo) jsonParser.getProducedType();
        FlatCols flatColObjs = new FlatCols();
        List<FlatCol> list = new ArrayList<>();
        for (int i = 0; i < cols.size(); i++) {
            FlatCol flatColObj = new FlatCol();
            String flatCol = cols.get(i);

            int flatColIndex = producedType.getFieldIndex(flatCol);
            if (flatColIndex == -1) {
                throw new Exception("field:" + flatCol + "不存在");
            }
            String[] outputFields;
            String[] flatFields = null;
            TypeInformation<Object> flatColType = producedType.getTypeAt(flatColIndex);
            if (flatColType instanceof ObjectArrayTypeInfo) {
                System.out.println("flatColType is array");
                System.out.println(flatColType.getTypeClass());
                System.out.println(flatColType.getClass());
                TypeInformation componentInfo = ((ObjectArrayTypeInfo) flatColType).getComponentInfo();
                if (componentInfo instanceof CompositeType) {
                    Class typeClass = ((CompositeType) componentInfo).getTypeClass();
                    if (typeClass.equals(Row.class)) {
                        TypeInformation<Row> componentInfoTypeRow = componentInfo;
                        RowTypeInfo rowTypeInfo = (RowTypeInfo) componentInfoTypeRow;
                        rowTypeInfo.getFieldNames();
                        flatFields = Arrays.copyOfRange(rowTypeInfo.getFieldNames(), 0, rowTypeInfo.getFieldNames().length);
                    }
                }
                flatColObj.setType("array");
            } else if (flatColType instanceof CompositeType) {
                Class typeClass = ((CompositeType) flatColType).getTypeClass();
                if (typeClass.equals(Row.class)) {
                    TypeInformation<Row> typeRow = producedType.getTypeAt(flatColIndex);
                    RowTypeInfo rowTypeInfo = (RowTypeInfo) typeRow;
                    rowTypeInfo.getFieldNames();
//                flatFields = new String[rowTypeInfo.getFieldNames().length];
                    flatFields = Arrays.copyOfRange(rowTypeInfo.getFieldNames(), 0, rowTypeInfo.getFieldNames().length);
                }
                System.out.println(typeClass);
                System.out.println(typeClass.getComponentType());
                flatColObj.setType("object");
            } else {
                throw new Exception("该类型不能flat！");
            }
            System.out.println(Arrays.toString(flatFields));

            flatColObj.setName(flatCol);
            flatColObj.setIndex(flatColIndex);
            if (flatFields != null && flatFields.length > 0) {
                flatColObj.setField(Arrays.asList(flatFields));
            }
            list.add(flatColObj);
        }

        Row row = jsonParser.deserialize(bytes);
        FlatCol flatCol = list.get(0);
        Object field = row.getField(flatCol.getIndex());
        List<String> fields = flatCol.getField();
        List<Row> flatRowList = new ArrayList<>();
        if (flatCol.getType().equals("object")) {
            Row flatRow = (Row) row.getField(flatCol.getIndex());
            flatRowList.add(flatRow);
        } else if (flatCol.getType().equals("array")) {
            Object[] objects = (Object[]) row.getField(flatCol.getIndex());
            for (Object temp : objects) {
                Row tempRow = (Row) temp;
                flatRowList.add(tempRow);
            }
        }


        List<Row> result = new ArrayList<>();
        for (int i = 0; i < flatRowList.size(); i++) {
            Row flatRow = flatRowList.get(i);
            Row resultRow = new Row(row.getArity() - 1 + flatRow.getArity());
            // 第一部分 0-->flatIndex
            for (int j = 0; j < flatCol.getIndex(); j++) {
                resultRow.setField(j, row.getField(j));
            }
            // 第二部分 flatIndex-->flatIndex+flatRow.length
            for (int j = 0; j < flatRow.getArity(); j++) {
                resultRow.setField(flatCol.getIndex() + j, flatRow.getField(j));
            }
            for (int j = 0; j < (resultRow.getArity() - flatRow.getArity() - flatCol.getIndex()); j++) {
                resultRow.setField(flatCol.getIndex() + flatRow.getArity() + j, row.getField(flatCol.getIndex() + 1 + j));
            }
            // 第三部分 flatIndex+flatRow.length-->resultRow.length
            result.add(resultRow);
        }

        System.out.println("---------result-----------");
//        System.out.printf("长度:%s\n", result.size());
        for (int i = 0; i < result.size(); i++) {
            System.out.printf("row长度:%s\n", result.get(i).getArity());
            System.out.println(result.get(i));
        }
    }

    public static List<String> getJsonStringList() {
        return Arrays.asList("{\"__db\":\"dcbsdb\",\"data\":[{\"x\":\"1\",\"y\":\"3\"},{\"x\":\"8\",\"y\":\"10\"},{\"x\":\"9\",\"y\":\"10\"}]}",
                "{\"__db\":\"dcbsdb\",\"data\":[{\"x\":\"1\",\"y\":\"2\"},{\"x\":\"1\",\"y\":\"14\"},{\"x\":\"3\",\"y\":\"11\"}]}");
    }

    public static String getJsonString() {
        return "{\n" +
                "\t\"__db\": \"dcbsdb\",\n" +
                "\t\"data\": [{\n" +
                "\t\t\"x\": \"1\",\n" +
                "\t\t\"y\": \"3\"\n" +
                "\t}, {\n" +
                "\t\t\"x\": \"8\",\n" +
                "\t\t\"y\": \"10\"\n" +
                "\t}, {\n" +
                "\t\t\"x\": \"9\",\n" +
                "\t\t\"y\": \"10\"\n" +
                "\t}],\n" +
                "\t\"data2\": {\n" +
                "\t\t\"x\": \"avc\",\n" +
                "\t\t\"y\": \"fsfs\"\n" +
                "\t}\n" +
                "}";
    }
}
