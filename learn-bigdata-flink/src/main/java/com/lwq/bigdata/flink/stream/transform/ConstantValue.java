package com.lwq.bigdata.flink.stream.transform;

/**
 * @author: LWQ
 * @create: 2020/12/16
 * @description: ConstantValue
 **/
public class ConstantValue {

    public static final String STAR_SYMBOL = "*";
    public static final String POINT_SYMBOL = ".";
    public static final String EQUAL_SYMBOL = "=";
    public static final String SINGLE_QUOTE_MARK_SYMBOL = "'";
    public static final String DOUBLE_QUOTE_MARK_SYMBOL = "\"";
    public static final String COMMA_SYMBOL = ",";

    public static final String SINGLE_SLASH_SYMBOL = "/";
    public static final String DOUBLE_SLASH_SYMBOL = "//";

    public static final String LEFT_PARENTHESIS_SYMBOL = "(";
    public static final String RIGHT_PARENTHESIS_SYMBOL = ")";

    public static final String KEY_HTTP = "http";

    public static final String PROTOCOL_HTTP = "http://";
    public static final String PROTOCOL_HTTPS = "https://";
    public static final String PROTOCOL_HDFS = "hdfs://";
    public static final String PROTOCOL_JDBC_MYSQL = "jdbc:mysql://";

    public static final String SYSTEM_PROPERTIES_KEY_OS = "os.name";
    public static final String SYSTEM_PROPERTIES_KEY_USER_DIR = "user.dir";
    public static final String SYSTEM_PROPERTIES_KEY_JAVA_VENDOR = "java.vendor";
    public static final String SYSTEM_PROPERTIES_KEY_FILE_ENCODING = "file.encoding";

    public static final String OS_WINDOWS = "windows";

    public static final String TIME_SECOND_SUFFIX = "sss";
    public static final String TIME_MILLISECOND_SUFFIX = "SSS";

    public static final String FILE_SUFFIX_XML = ".xml";

    public static final int MAX_BATCH_SIZE = 200000;

    public static final long STORE_SIZE_G = 1024L * 1024 * 1024;

    public static final long STORE_SIZE_M = 1024L * 1024;
}
