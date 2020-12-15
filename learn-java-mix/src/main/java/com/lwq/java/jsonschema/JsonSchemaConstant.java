package com.lwq.java.jsonschema;

/**
 * @author: LWQ
 * @create: 2020/12/7
 * @description: JsonSchemaConstant
 **/
public class JsonSchemaConstant {
    public static final String SCHEMA = "{\n" +
            "    \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n" +
            "    \"type\": \"object\",\n" +
            "    \"properties\": {\n" +
            "        \"__db\": {\n" +
            "            \"type\": \"string\"\n" +
            "        },\n" +
            "        \"__table\": {\n" +
            "            \"type\": \"string\"\n" +
            "        },\n" +
            "        \"__binlogCurtGTID\": {\n" +
            "            \"type\": \"string\"\n" +
            "        },\n" +
            "        \"__binlogTime\": {\n" +
            "            \"type\": \"null\"\n" +
            "        },\n" +
            "        \"__type\": {\n" +
            "            \"type\": \"string\"\n" +
            "        },\n" +
            "        \"data\": {\n" +
            "            \"type\": \"array\",\n" +
            "            \"items\": {\n" +
            "                \"type\": \"object\",\n" +
            "                \"properties\": {\n" +
            "                    \"TRAN_INNR_SQNUM\": {\n" +
            "                        \"type\": \"string\"\n" +
            "                    },\n" +
            "                    \"CRNT_ACCT_DATE\": {\n" +
            "                        \"type\": \"string\"\n" +
            "                    },\n" +
            "                    \"BANK_NUM\": {\n" +
            "                        \"type\": \"string\"\n" +
            "                    },\n" +
            "                    \"TRAN_LOG_NUM\": {\n" +
            "                        \"type\": \"string\"\n" +
            "                    }\n" +
            "                },\n" +
            "                \"additionalProperties\": false,\n" +
            "                \"required\": [\n" +
            "                    \"TRAN_INNR_SQNUM\",\n" +
            "                    \"CRNT_ACCT_DATE\",\n" +
            "                    \"BANK_NUM\",\n" +
            "                    \"TRAN_LOG_NUM\"\n" +
            "                ]\n" +
            "            },\n" +
            "            \"additionalItems\": false\n" +
            "        }\n" +
            "    },\n" +
            "    \"additionalProperties\": false,\n" +
            "    \"required\": [\n" +
            "        \"__db\",\n" +
            "        \"__table\",\n" +
            "        \"__binlogCurtGTID\",\n" +
            "        \"__binlogTime\",\n" +
            "        \"__type\",\n" +
            "        \"data\"\n" +
            "    ]\n" +
            "}";
}
