/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lwq.bigdata.flink.format.json;

import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JsonNodeDeserializationSchema}.
 */
public class JsonNodeDeserializationSchemaTest {

	@Test
	public void testDeserialize() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode initialValue = mapper.createObjectNode();
		initialValue.put("key", 4).put("value", "world");
		byte[] serializedValue = mapper.writeValueAsBytes(initialValue);
		System.out.println(initialValue);

		JsonNodeDeserializationSchema schema = new JsonNodeDeserializationSchema();
		ObjectNode deserializedValue = schema.deserialize(serializedValue);

		System.out.println(schema);
		System.out.println(deserializedValue);
		assertEquals(4, deserializedValue.get("key").asInt());
		assertEquals("world", deserializedValue.get("value").asText());
	}
}
