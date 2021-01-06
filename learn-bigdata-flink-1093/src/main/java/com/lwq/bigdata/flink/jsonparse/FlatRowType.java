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
    }


    public static Row assembleRow(Row oriRow, RowTypeInfo rowTypeInfo, Map<String, Integer> indexMap) {
        Row result = new Row(rowTypeInfo.getArity());
        int arity = rowTypeInfo.getArity();
        for (int i = 0; i < arity; i++) {
            String path = rowTypeInfo.getFieldNames()[i];
            TypeInformation typeInformation = rowTypeInfo.getTypeAt(i);
            if (typeInformation instanceof ObjectArrayTypeInfo) {

            } else {
            }
        }
        return result;
    }

    public static Object getObjectByPath(Row oriRow, String path, Map<String, Integer> indexMap) {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        String[] paths = path.split("/");
        int length = paths.length;
        int curr = 0;
        Object currObj = oriRow;
        while (curr < length) {
            String currPath = paths[curr];
            if (!indexMap.containsKey(currPath)) {
                //TODO
            }
            int pathIndex = indexMap.get(currPath);
            if (currObj instanceof Row) {

            }
            curr++;
        }
        return null;
    }

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
