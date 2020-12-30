package com.lwq.bigdata.flink.stream.transform;

import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: StringUtil
 **/
public class StringUtil {

    public static final int STEP_SIZE = 2;

    /**
     * Handle the escaped escape charactor.
     * <p>
     * e.g. Turnning \\t into \t, etc.
     *
     * @param str The String to convert
     * @return the converted String
     */
    public static String convertRegularExpr(String str) {
        if (str == null) {
            return "";
        }

        String pattern = "\\\\(\\d{3})";

        Pattern r = Pattern.compile(pattern);
        while (true) {
            Matcher m = r.matcher(str);
            if (!m.find()) {
                break;
            }
            String num = m.group(1);
            int x = Integer.parseInt(num, 8);
            str = m.replaceFirst(String.valueOf((char) x));
        }
        str = str.replaceAll("\\\\t", "\t");
        str = str.replaceAll("\\\\r", "\r");
        str = str.replaceAll("\\\\n", "\n");

        return str;
    }

    public static Object string2col(String str, String type, SimpleDateFormat customTimeFormat) {
        if (str == null || str.length() == 0 || type == null) {
            return str;
        }

        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        Object ret;
        switch (columnType) {
            case TINYINT:
                ret = Byte.valueOf(str.trim());
                break;
            case SMALLINT:
                ret = Short.valueOf(str.trim());
                break;
            case INT:
                ret = Integer.valueOf(str.trim());
                break;
            case MEDIUMINT:
            case BIGINT:
                ret = Long.valueOf(str.trim());
                break;
            case FLOAT:
                ret = Float.valueOf(str.trim());
                break;
            case DOUBLE:
                ret = Double.valueOf(str.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if (customTimeFormat != null) {
                    ret = DateUtil.columnToDate(str, customTimeFormat);
                    ret = DateUtil.timestampToString((Date) ret);
                } else {
                    ret = str;
                }
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.trim().toLowerCase());
                break;
            case DATE:
                ret = DateUtil.columnToDate(str, customTimeFormat);
                break;
            case TIMESTAMP:
            case DATETIME:
                ret = DateUtil.columnToTimestamp(str, customTimeFormat);
                break;
            default:
                ret = str;
        }

        return ret;
    }

    public static String col2string(Object column, String type) {
        if (column == null) {
            return "";
        }

        if (type == null) {
            return column.toString();
        }

        String rowData = column.toString();
        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        Object result;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData.trim());
                break;
            case SMALLINT:
            case SHORT:
                result = Short.valueOf(rowData.trim());
                break;
            case INT:
            case INTEGER:
                result = Integer.valueOf(rowData.trim());
                break;
            case BIGINT:
            case LONG:
                if (column instanceof Timestamp) {
                    result = ((Timestamp) column).getTime();
                } else {
                    result = Long.valueOf(rowData.trim());
                }
                break;
            case FLOAT:
                result = Float.valueOf(rowData.trim());
                break;
            case DOUBLE:
                result = Double.valueOf(rowData.trim());
                break;
            case DECIMAL:
                result = new BigDecimal(rowData.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
            case TEXT:
                if (column instanceof Timestamp) {
                    result = DateUtil.timestampToString((java.util.Date) column);
                } else {
                    result = rowData;
                }
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData.trim());
                break;
            case DATE:
                result = DateUtil.dateToString(DateUtil.columnToDate(column, null));
                break;
            case DATETIME:
            case TIMESTAMP:
                result = DateUtil.timestampToString(DateUtil.columnToTimestamp(column, null));
                break;
            default:
                result = rowData;
        }
        return result.toString();
    }


    public static String row2string(Row row, List<String> columnTypes, String delimiter) throws Exception {
        // convert row to string
        int size = row.getArity();
        StringBuilder sb = new StringBuilder(128);

        int i = 0;
        try {
            for (; i < size; ++i) {
                if (i != 0) {
                    sb.append(delimiter);
                }

                Object column = row.getField(i);

                if (column == null) {
                    continue;
                }

                sb.append(col2string(column, columnTypes.get(i)));
            }
        } catch (Exception e) {
            String msg = "StringUtil.row2string error: when converting field[" + i + "] in Row(" + row + ")";
            throw new Exception(msg);
        }

        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String hexString) {
        if (hexString == null) {
            return null;
        }

        int length = hexString.length();

        byte[] bytes = new byte[length / 2];
        for (int i = 0; i < length; i += STEP_SIZE) {
            bytes[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }

        return bytes;
    }
}
