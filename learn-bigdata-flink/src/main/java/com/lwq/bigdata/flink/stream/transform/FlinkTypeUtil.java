package com.lwq.bigdata.flink.stream.transform;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: FlinkTypeUtil
 **/
public class FlinkTypeUtil {

    public static TypeInformation convertType(String type) {
        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        TypeInformation typeInformation;
        switch (columnType) {
            case TINYINT:
                typeInformation = BasicTypeInfo.INT_TYPE_INFO;
                break;
            case SMALLINT:
                typeInformation = BasicTypeInfo.INT_TYPE_INFO;
                break;
            case INT:
                typeInformation = BasicTypeInfo.INT_TYPE_INFO;
                break;
            case MEDIUMINT:
            case BIGINT:
                typeInformation = BasicTypeInfo.LONG_TYPE_INFO;
                break;
            case FLOAT:
                typeInformation = BasicTypeInfo.FLOAT_TYPE_INFO;
                break;
            case DOUBLE:
                typeInformation = BasicTypeInfo.DOUBLE_TYPE_INFO;
                break;
            case STRING:
                typeInformation = BasicTypeInfo.STRING_TYPE_INFO;
            case VARCHAR:
                typeInformation = BasicTypeInfo.STRING_TYPE_INFO;
            case CHAR:
                typeInformation = BasicTypeInfo.STRING_TYPE_INFO;
                break;
            case BOOLEAN:
                typeInformation = BasicTypeInfo.BOOLEAN_TYPE_INFO;
                break;
            case DATE:
                typeInformation = BasicTypeInfo.DATE_TYPE_INFO;
                break;
            case TIMESTAMP:
                typeInformation = BasicTypeInfo.DATE_TYPE_INFO;
            case DATETIME:
                typeInformation = BasicTypeInfo.DATE_TYPE_INFO;
                break;
            default:
                typeInformation = BasicTypeInfo.STRING_TYPE_INFO;
        }

        return typeInformation;
    }
}
