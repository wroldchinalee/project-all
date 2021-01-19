package com.lwq.bigdata.flink.jsonparse;

import com.lwq.bigdata.flink.format.utils.JsonSchemaHolder;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author: LWQ
 * @create: 2021/1/5
 * @description: FlatRowType
 **/
public class FlatRowType {
    public static void main(String[] args) throws IOException {
        JsonRowDeserializationSchema jsonParser = new JsonRowDeserializationSchema.Builder(JsonSchemaHolder.JSON_SCHEMA7).build();
        Row row = jsonParser.deserialize(json.getBytes("UTF-8"));
        RowTypeInfo producedType = ((RowTypeInfo) jsonParser.getProducedType());
        System.out.println("productType:" + producedType);
        HashMap<String, Integer> indexMap = new HashMap<>();
        RowTypeInfo rowTypeInfo = flatRowTypeInfo(producedType, "<root>", indexMap);
        System.out.println(rowTypeInfo);
        for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
            System.out.println(entry);
        }
        Row result = assembleRow(row, rowTypeInfo, indexMap);
        System.out.println(result);
        int flatCol = 2;
        Object field = result.getField(flatCol);
        System.out.println(field);
        List<Row> resultList = flatRow(result, rowTypeInfo, flatCol);
        System.out.println("-------------result------------");
        for (int i = 0; i < resultList.size(); i++) {
            System.out.println(resultList.get(i));
        }

    }

    public static List<Row> flatRow(Row oriRow, RowTypeInfo rowTypeInfo, int flatCol) {
        Object obj = oriRow.getField(flatCol);
        TypeInformation objType = rowTypeInfo.getTypeAt(flatCol);
        if (!(objType instanceof ObjectArrayTypeInfo) || !objType.getTypeClass().equals(Row[].class)) {
            return Arrays.asList(oriRow);
        }
        TypeInformation<Row> componentInfo = ((ObjectArrayTypeInfo) objType).getComponentInfo();
        String arrayFieldName = rowTypeInfo.getFieldNames()[flatCol];
        RowTypeInfo elemTypeInfo = (RowTypeInfo) componentInfo;
        String[] fieldNames = elemTypeInfo.getFieldNames();
        Object[] objectArray = (Object[]) oriRow.getField(flatCol);
        ArrayList<Row> result = new ArrayList<>();
        for (int i = 0; i < objectArray.length; i++) {
            Row flatRow = ((Row) objectArray[i]);
            Row resultRow = new Row(rowTypeInfo.getArity() + fieldNames.length - 1);
            // 第一部分 0-->flatIndex
            for (int j = 0; j < flatCol; j++) {
                resultRow.setField(j, oriRow.getField(j));
            }
            // 第二部分 flatIndex-->flatIndex+flatRow.length
            for (int j = 0; j < flatRow.getArity(); j++) {
                resultRow.setField(flatCol + j, flatRow.getField(j));
            }
            for (int j = 0; j < (resultRow.getArity() - flatRow.getArity() - flatCol); j++) {
                resultRow.setField(flatCol + flatRow.getArity() + j, oriRow.getField(flatCol + 1 + j));
            }
            // 第三部分 flatIndex+flatRow.length-->resultRow.length
            result.add(resultRow);
        }
        return result;
    }

    public static Row assembleRow(Row oriRow, RowTypeInfo rowTypeInfo, Map<String, Integer> indexMap) {
        Row result = new Row(rowTypeInfo.getArity());
        int arity = rowTypeInfo.getArity();
        for (int i = 0; i < arity; i++) {
            String path = rowTypeInfo.getFieldNames()[i];
            Object obj = getObjectByPath(oriRow, path, indexMap);
            ;
            result.setField(i, obj);
        }
        return result;
    }

    public static Object getObjectByPath(Row oriRow, String path, Map<String, Integer> indexMap) {
        if (Objects.isNull(oriRow) || StringUtils.isEmpty(path)) {
            return null;
        }
        if (path.startsWith("<root>/")) {
            int rootIndex = path.indexOf("/");
            path = path.substring(rootIndex + 1);
        }
        int count = 0;
        String[] paths = path.split("/");
        int length = paths.length;
        Object currObj = oriRow;
        for (int i = 0; i < length; i++) {
            String currPath = paths[i];
            if (!indexMap.containsKey(currPath)) {
                //TODO
            }
            int index = indexMap.get(currPath);
            if (currObj instanceof Row) {
                currObj = ((Row) currObj).getField(index);
            }
            count++;
        }
        return currObj;
    }

//    public static Object getObjectByPath2(Row oriRow, String path, Map<String, Integer> indexMap) {
//        if (StringUtils.isEmpty(path)) {
//            return null;
//        }
//        String[] paths = path.split("/");
//        int length = paths.length;
//        int curr = 0;
//        Object currObj = oriRow;
//        while (curr < length) {
//            String currPath = paths[curr];
//            if (!indexMap.containsKey(currPath)) {
//                //TODO
//            }
//            int pathIndex = indexMap.get(currPath);
//            if (currObj instanceof Row) {
//
//            }
//            curr++;
//        }
//        return null;
//    }

    public static RowTypeInfo flatRowTypeInfo(RowTypeInfo rowTypeInfo, String location, Map<String, Integer> indexMap) {
        if (rowTypeInfo == null) {
            return null;
        }
        ArrayList<TypeInformation> typeInformations = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        int arity = rowTypeInfo.getArity();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < arity; i++) {
            if (rowTypeInfo.getTypeAt(i).getTypeClass().equals(Row.class)) {
                TypeInformation<Row> typeInformation = rowTypeInfo.getTypeAt(i);
                RowTypeInfo innerRowTypeInfo = ((RowTypeInfo) typeInformation);
                RowTypeInfo innerResult = flatRowTypeInfo(innerRowTypeInfo, location + "/" + fieldNames[i], indexMap);

                names.addAll(Arrays.asList(innerResult.getFieldNames()));
                typeInformations.addAll(Arrays.asList(innerResult.getFieldTypes()));
                indexMap.put(fieldNames[i], i);
            } else {
                indexMap.put(fieldNames[i], i);
                names.add(location + "/" + fieldNames[i]);
                typeInformations.add(rowTypeInfo.getTypeAt(i));
            }
        }
        TypeInformation[] arr1 = new TypeInformation[typeInformations.size()];
        String[] arr2 = new String[names.size()];

        return new RowTypeInfo(typeInformations.toArray(arr1), names.toArray(arr2));
    }

    public static RowTypeInfo flatRowTypeInfo2(RowTypeInfo rowTypeInfo, String location) {
        if (rowTypeInfo == null) {
            return null;
        }
        ArrayList<TypeInformation> typeInformations = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        int arity = rowTypeInfo.getArity();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < arity; i++) {
            if (rowTypeInfo.getTypeAt(i).getTypeClass().equals(Row.class)) {
                TypeInformation<Row> typeInformation = rowTypeInfo.getTypeAt(i);
                RowTypeInfo innerRowTypeInfo = ((RowTypeInfo) typeInformation);
                RowTypeInfo innerResult = flatRowTypeInfo2(innerRowTypeInfo, location + "/" + fieldNames[i]);

                names.addAll(Arrays.asList(innerResult.getFieldNames()));
                typeInformations.addAll(Arrays.asList(innerResult.getFieldTypes()));
            } else {
                names.add(location + "/" + fieldNames[i]);
                typeInformations.add(rowTypeInfo.getTypeAt(i));
            }
        }
        TypeInformation[] arr1 = new TypeInformation[typeInformations.size()];
        String[] arr2 = new String[names.size()];

        return new RowTypeInfo(typeInformations.toArray(arr1), names.toArray(arr2));
    }

    public static List<String> flatFields(RowTypeInfo rowTypeInfo, String location) {
        if (rowTypeInfo == null) {
            return Lists.newArrayList();
        }
        ArrayList<String> result = new ArrayList<>();
        int arity = rowTypeInfo.getArity();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < arity; i++) {
            if (rowTypeInfo.getTypeAt(i).getTypeClass().equals(Row.class)) {
                TypeInformation<Row> typeInformation = rowTypeInfo.getTypeAt(i);
                RowTypeInfo innerRowTypeInfo = ((RowTypeInfo) typeInformation);
                List<String> innerResult = flatFields(innerRowTypeInfo, location + "/" + fieldNames[i]);
                result.addAll(innerResult);
            } else {
                result.add(location + "/" + fieldNames[i]);
            }
        }
        return result;
    }

    private static final String json = "{\"aa\":{\"bb\":{\"cc\":123,\"dd\":\"dd1\",\"ff\":[{\"gg\":\"gg1\",\"hh\":\"hh1\"},{\"gg\":\"gg2\",\"hh\":\"hh2\"}]},\"ee\":{\"jj\":\"jj1\",\"kk\":\"kk1\"},\"ll\":\"ll1\"},\"ii\": \"ii1\",\"mm\":[\"mm1\",\"mm2\",\"mm3\"]}";
}
