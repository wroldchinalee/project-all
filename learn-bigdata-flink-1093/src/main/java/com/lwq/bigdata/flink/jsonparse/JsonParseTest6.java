package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/12/23
 * @description: JsonParseTest3
 **/
public class JsonParseTest6 {
    public static void main(String[] args) throws Exception {
        JsonRowDeserializationSchema jsonParser = new JsonRowDeserializationSchema.Builder(JsonSchemaHolder.JSON_SCHEMA7).build();
        Row row = jsonParser.deserialize(json.getBytes("UTF-8"));
        RowTypeInfo producedType = ((RowTypeInfo) jsonParser.getProducedType());
        System.out.println("productType:" + producedType);
        int fieldNum = producedType.getArity();
        ArrayList<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < fieldNum; i++) {
            String fieldName = producedType.getFieldNames()[i];
            TypeInformation<Object> typeAt = producedType.getTypeAt(i);
            if (typeAt instanceof TupleTypeInfoBase) {
                if (typeAt.getTypeClass().equals(Row.class)) {
                    TypeInformation<Row> rowTypeAt = producedType.getTypeAt(i);
                    RowTypeInfo rowTypeInfo = (RowTypeInfo) rowTypeAt;

                }
                System.out.println(((CompositeType) typeAt).getTypeClass());

//                CompositeType<Row> type = typeAt;
            } else if (typeAt instanceof ObjectArrayTypeInfo) {

            }
        }


//        ArrayList<String> pathList = new ArrayList<>();
//
//        int splitIndex = -1;
//        for (int i = 0; i < fieldNum; i++) {
//            String fieldName = producedType.getFieldNames()[i];
//            TypeInformation typeInformation = producedType.getTypeAt(i);
//            System.out.printf("fieldName:%s,type:%s\n", fieldName, typeInformation);
//            if (typeInformation instanceof ObjectArrayTypeInfo && ((ObjectArrayTypeInfo) typeInformation).getComponentInfo() instanceof RowTypeInfo) {
//                splitIndex = i;
//                System.out.println(typeInformation.getTypeClass());
//                System.out.println("i'm array type!");
//                RowTypeInfo componentInfo = ((RowTypeInfo) ((ObjectArrayTypeInfo) typeInformation).getComponentInfo());
//                int innerArity = componentInfo.getArity();
//                for (int j = 0; j < innerArity; j++) {
//                    String path = fieldName + "/" + componentInfo.getFieldNames()[j];
//                    pathList.add(path);
//                }
//                continue;
//            }
//            if (typeInformation instanceof BasicArrayTypeInfo) {
//                System.out.println(typeInformation.getTypeClass());
//                System.out.println("i'm basic array type!");
//            }
//            pathList.add(fieldName);
//        }
//
//        System.out.println("-----------------------");
//        for (String path : pathList) {
//            System.out.println(path);
//        }
//        System.out.println("-----------------------");
//
//
////        System.out.println(Arrays.toString(producedType.getFieldNames()));
//        Row row = jsonParser.deserialize(json.getBytes("UTF-8"));
//
//        if (splitIndex != -1) {
//            Object[] rows = (Object[]) row.getField(splitIndex);
//            ArrayList<Row> flatRowList = new ArrayList<>();
//            for (Object o : rows) {
//                flatRowList.add((Row) o);
//            }
//            System.out.println(flatRowList);
//
//            List<Row> result = new ArrayList<>();
//            for (int i = 0; i < flatRowList.size(); i++) {
//                Row flatRow = flatRowList.get(i);
//                Row resultRow = new Row(pathList.size());
//                // 第一部分 0-->flatIndex
//                for (int j = 0; j < splitIndex; j++) {
//                    resultRow.setField(j, row.getField(j));
//                }
//                // 第二部分 flatIndex-->flatIndex+flatRow.length
//                for (int j = 0; j < flatRow.getArity(); j++) {
//                    resultRow.setField(splitIndex + j, flatRow.getField(j));
//                }
//                for (int j = 0; j < (resultRow.getArity() - flatRow.getArity() - splitIndex); j++) {
//                    resultRow.setField(splitIndex + flatRow.getArity() + j, row.getField(splitIndex + 1 + j));
//                }
//                // 第三部分 flatIndex+flatRow.length-->resultRow.length
//                result.add(resultRow);
//            }
//            System.out.println("------------------result-----------------");
//            for (String path : pathList) {
//                System.out.printf("%s ", path);
//            }
//            System.out.println();
//            for (Row temp : result) {
//                System.out.println(temp);
//            }
//        }

    }

    public static NRow flatRow(Row row, RowTypeInfo rowTypeInfo) {
        if (row == null || rowTypeInfo == null) {
            return null;
        }
        int arity = row.getArity();
        for (int i = 0; i < arity; i++) {
            TypeInformation<Object> typeAt = rowTypeInfo.getTypeAt(i);
            if (typeAt instanceof CompositeType && typeAt.getTypeClass().equals(Row.class)) {
                TypeInformation<Row> innerTypeInfo = rowTypeInfo.getTypeAt(i);
                RowTypeInfo innerRowTypeInfo = (RowTypeInfo) innerTypeInfo;
                Row innerRow = (Row) row.getField(i);
                NRow nRow = new NRow(innerRow, innerRowTypeInfo.getFieldNames());
            } else {

            }
        }
        return null;
    }


    private static final String json = "{\"aa\":{\"bb\":{\"cc\":123,\"dd\":\"dd1\",\"ff\":[{\"gg\":\"gg1\",\"hh\":\"hh1\"},{\"gg\":\"gg2\",\"hh\":\"hh2\"}]},\"ee\":{\"jj\":\"jj1\",\"kk\":\"kk1\"},\"ll\":\"ll1\"},\"ii\": \"ii1\",\"mm\":[\"mm1\",\"mm2\",\"mm3\"]}";
}
