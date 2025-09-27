/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet.impl.read.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VariantReadTest {

    private VariantRead variantRead;

    @BeforeEach
    void setUp() {
        variantRead = new VariantRead();
    }

    @Test
    void deserializeNullValue() {
        // Test null variant
        Object result = variantRead.deserialize(null);
        assertNull(result);
    }

    @Test
    void deserializeNullType() {
        // Test NULL type variant
        VariantBuilder builder = new VariantBuilder();
        builder.appendNull();

        Object nullResult = variantRead.deserialize(builder.build());
        assertNull(nullResult);
    }

    @Test
    void deserializeBoolean() {
        // Test true
        VariantBuilder builder = new VariantBuilder();
        builder.appendBoolean(true);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);

        // Test false
        builder = new VariantBuilder();
        builder.appendBoolean(false);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Boolean);
        assertFalse((Boolean) result);
    }

    @Test
    void deserializeByte() {
        byte testValue = 42;
        VariantBuilder builder = new VariantBuilder();
        builder.appendByte(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Byte);
        assertEquals(testValue, (Byte) result);

        // Test negative byte
        testValue = -128;
        builder = new VariantBuilder();
        builder.appendByte(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Byte);
        assertEquals(testValue, (Byte) result);
    }

    @Test
    void deserializeShort() {
        short testValue = 1234;
        VariantBuilder builder = new VariantBuilder();
        builder.appendShort(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Short);
        assertEquals(testValue, (Short) result);

        // Test negative short
        testValue = -32768;
        builder = new VariantBuilder();
        builder.appendShort(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Short);
        assertEquals(testValue, (Short) result);
    }

    @Test
    void deserializeInt() {
        int testValue = 1234567890;
        VariantBuilder builder = new VariantBuilder();
        builder.appendInt(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Integer);
        assertEquals(testValue, (Integer) result);

        // Test negative int
        testValue = Integer.MIN_VALUE;
        builder = new VariantBuilder();
        builder.appendInt(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Integer);
        assertEquals(testValue, (Integer) result);
    }

    @Test
    void deserializeLong() {
        long testValue = 1234567890987654321L;
        VariantBuilder builder = new VariantBuilder();
        builder.appendLong(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Long);
        assertEquals(testValue, (Long) result);

        // Test negative long
        testValue = Long.MIN_VALUE;
        builder = new VariantBuilder();
        builder.appendLong(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Long);
        assertEquals(testValue, (Long) result);
    }

    @Test
    void deserializeFloat() {
        float testValue = 3.14159f;
        VariantBuilder builder = new VariantBuilder();
        builder.appendFloat(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Float);
        assertEquals(testValue, (Float) result, 0.0001f);

        // Test negative float
        testValue = -123.456f;
        builder = new VariantBuilder();
        builder.appendFloat(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Float);
        assertEquals(testValue, (Float) result, 0.0001f);
    }

    @Test
    void deserializeDouble() {
        double testValue = 3.141592653589793;
        VariantBuilder builder = new VariantBuilder();
        builder.appendDouble(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Double);
        assertEquals(testValue, (Double) result, 0.0000001);

        // Test negative double
        testValue = -123.456789;
        builder = new VariantBuilder();
        builder.appendDouble(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Double);
        assertEquals(testValue, (Double) result, 0.0000001);
    }

    @Test
    void deserializeString() {
        String testValue = "Hello, World!";
        VariantBuilder builder = new VariantBuilder();
        builder.appendString(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof String);
        assertEquals(testValue, result);

        // Test empty string
        testValue = "";
        builder = new VariantBuilder();
        builder.appendString(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof String);
        assertEquals(testValue, result);

        // Test special characters
        testValue = "Special chars: áéíóú ñ ¡¿";
        builder = new VariantBuilder();
        builder.appendString(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof String);
        assertEquals(testValue, result);
    }

    @Test
    void deserializeDecimal() {
        // Test different decimal precisions
        BigDecimal testValue = new BigDecimal("123.456");
        VariantBuilder builder = new VariantBuilder();
        builder.appendDecimal(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof BigDecimal);
        assertEquals(testValue, result);

        // Test large decimal
        testValue = new BigDecimal("123456789.987654321");
        builder = new VariantBuilder();
        builder.appendDecimal(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof BigDecimal);
        assertEquals(testValue, result);

        // Test negative decimal
        testValue = new BigDecimal("-987.654");
        builder = new VariantBuilder();
        builder.appendDecimal(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof BigDecimal);
        assertEquals(testValue, result);
    }

    @Test
    void deserializeUUID() {
        UUID testValue = UUID.randomUUID();
        VariantBuilder builder = new VariantBuilder();
        builder.appendUUID(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof UUID);
        assertEquals(testValue, result);

        // Test specific UUID
        testValue = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        builder = new VariantBuilder();
        builder.appendUUID(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof UUID);
        assertEquals(testValue, result);
    }

    @Test
    void deserializeBinary() {
        byte[] testBytes = { 0x01, 0x02, 0x03, 0x04, 0x05 };
        ByteBuffer testValue = ByteBuffer.wrap(testBytes);
        VariantBuilder builder = new VariantBuilder();
        builder.appendBinary(testValue);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Binary);
        Binary resultBinary = (Binary) result;

        assertEquals(Binary.fromConstantByteArray(testBytes), resultBinary);

        // Test empty binary
        testBytes = new byte[0];
        testValue = ByteBuffer.wrap(testBytes);
        builder = new VariantBuilder();
        builder.appendBinary(testValue);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Binary);
        resultBinary = (Binary) result;

        assertEquals(Binary.fromConstantByteArray(testBytes), resultBinary);
    }

    @Test
    void deserializeDate() {
        // Test specific date
        LocalDate expectedDate = LocalDate.of(2025, 4, 17);
        int daysFromEpoch = (int) expectedDate.toEpochDay();

        VariantBuilder builder = new VariantBuilder();
        builder.appendDate(daysFromEpoch);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalDate);
        assertEquals(expectedDate, result);

        // Test epoch date
        expectedDate = LocalDate.ofEpochDay(0);
        builder = new VariantBuilder();
        builder.appendDate(0);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalDate);
        assertEquals(expectedDate, result);

        // Test negative date (before epoch)
        expectedDate = LocalDate.of(1969, 12, 31);
        daysFromEpoch = (int) expectedDate.toEpochDay();
        builder = new VariantBuilder();
        builder.appendDate(daysFromEpoch);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalDate);
        assertEquals(expectedDate, result);
    }

    @Test
    void deserializeTime() {
        // Test specific time
        LocalTime expectedTime = LocalTime.of(12, 30, 45, 123456000);
        long microsFromMidnight = expectedTime.toNanoOfDay() / 1000L;

        VariantBuilder builder = new VariantBuilder();
        builder.appendTime(microsFromMidnight);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalTime);
        assertEquals(expectedTime, result);

        // Test midnight
        expectedTime = LocalTime.MIDNIGHT;
        builder = new VariantBuilder();
        builder.appendTime(0L);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalTime);
        assertEquals(expectedTime, result);

        // Test end of day
        expectedTime = LocalTime.of(23, 59, 59, 999999000);
        microsFromMidnight = expectedTime.toNanoOfDay() / 1000L;
        builder = new VariantBuilder();
        builder.appendTime(microsFromMidnight);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalTime);
        assertEquals(expectedTime, result);
    }

    @Test
    void deserializeTimestampTz() {
        // Test specific timestamp with timezone
        long testMicros = 1734373425321456L;

        VariantBuilder builder = new VariantBuilder();
        builder.appendTimestampTz(testMicros);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Instant);

        Instant expected = Instant.ofEpochSecond(testMicros / 1_000_000L, (testMicros % 1_000_000L) * 1000L);
        assertEquals(expected, result);

        // Test epoch timestamp
        builder = new VariantBuilder();
        builder.appendTimestampTz(0L);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Instant);
        assertEquals(Instant.EPOCH, result);
    }

    @Test
    void deserializeTimestampNanosTz() {
        // Test specific timestamp with nanoseconds and timezone
        long testNanos = 1734373425321456789L;

        VariantBuilder builder = new VariantBuilder();
        builder.appendTimestampNanosTz(testNanos);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Instant);

        Instant expected = Instant.ofEpochSecond(testNanos / 1_000_000_000L, testNanos % 1_000_000_000L);
        assertEquals(expected, result);

        // Test epoch timestamp
        builder = new VariantBuilder();
        builder.appendTimestampNanosTz(0L);

        result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Instant);
        assertEquals(Instant.EPOCH, result);
    }

    @Test
    void deserializeTimestampNtz() {
        // Test specific timestamp without timezone (converted from micros to nanos)
        long testMicros = 1734373425321456L;

        VariantBuilder builder = new VariantBuilder();
        builder.appendTimestampNtz(testMicros);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalDateTime);

        Instant instant = InstantRead.instantFromMicrosFromEpoch(testMicros * 1000L);
        LocalDateTime expected = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        assertEquals(expected, result);
    }

    @Test
    void deserializeTimestampNanosNtz() {
        // Test specific timestamp with nanoseconds without timezone
        long testNanos = 1734373425321456789L;

        VariantBuilder builder = new VariantBuilder();
        builder.appendTimestampNanosNtz(testNanos);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof LocalDateTime);

        LocalDateTime expected = LocalDateTime.ofEpochSecond(testNanos / 1_000_000_000L,
                (int) (testNanos % 1_000_000_000L), ZoneOffset.UTC);
        assertEquals(expected, result);
    }

    @Test
    void deserializeEmptyArray() {
        VariantBuilder builder = new VariantBuilder();
        builder.startArray();
        builder.endArray();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof List);
        List<?> list = (List<?>) result;
        assertTrue(list.isEmpty());
    }

    @Test
    void deserializeArrayWithPrimitives() {
        VariantBuilder builder = new VariantBuilder();
        VariantArrayBuilder arrayBuilder = builder.startArray();
        arrayBuilder.appendString("test");
        arrayBuilder.appendInt(42);
        arrayBuilder.appendBoolean(true);
        arrayBuilder.appendNull();
        builder.endArray();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof List);
        List<?> list = (List<?>) result;
        assertEquals(4, list.size());
        assertEquals("test", list.get(0));
        assertEquals(42, list.get(1));
        assertEquals(true, list.get(2));
        assertNull(list.get(3));
    }

    @Test
    void deserializeNestedArray() {
        VariantBuilder builder = new VariantBuilder();
        VariantArrayBuilder arrayBuilder = builder.startArray();
        arrayBuilder.appendString("outer");

        // Create nested array
        VariantArrayBuilder nestedArrayBuilder = arrayBuilder.startArray();
        nestedArrayBuilder.appendInt(1);
        nestedArrayBuilder.appendInt(2);
        nestedArrayBuilder.appendString("nested");
        arrayBuilder.endArray();

        builder.endArray();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof List);
        List<?> outerList = (List<?>) result;
        assertEquals(2, outerList.size());
        assertEquals("outer", outerList.get(0));

        assertTrue(outerList.get(1) instanceof List);
        List<?> nestedList = (List<?>) outerList.get(1);
        assertEquals(3, nestedList.size());
        assertEquals(1, nestedList.get(0));
        assertEquals(2, nestedList.get(1));
        assertEquals("nested", nestedList.get(2));
    }

    @Test
    void deserializeEmptyObject() {
        VariantBuilder builder = new VariantBuilder();
        builder.startObject();
        builder.endObject();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Map);
        Map<?, ?> map = (Map<?, ?>) result;
        assertTrue(map.isEmpty());
    }

    @Test
    void deserializeObjectWithFields() {
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder objectBuilder = builder.startObject();
        objectBuilder.appendKey("name");
        objectBuilder.appendString("John");
        objectBuilder.appendKey("age");
        objectBuilder.appendInt(30);
        objectBuilder.appendKey("active");
        objectBuilder.appendBoolean(true);
        objectBuilder.appendKey("data");
        objectBuilder.appendNull();
        builder.endObject();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Map);
        Map<?, ?> map = (Map<?, ?>) result;
        assertEquals(4, map.size());
        assertEquals("John", map.get("name"));
        assertEquals(30, map.get("age"));
        assertEquals(true, map.get("active"));
        assertNull(map.get("data"));
    }

    @Test
    void deserializeNestedObject() {
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder objectBuilder = builder.startObject();
        objectBuilder.appendKey("user");
        objectBuilder.appendString("admin");

        objectBuilder.appendKey("details");
        VariantObjectBuilder nestedObjectBuilder = objectBuilder.startObject();
        nestedObjectBuilder.appendKey("id");
        nestedObjectBuilder.appendInt(123);
        nestedObjectBuilder.appendKey("type");
        nestedObjectBuilder.appendString("administrator");
        objectBuilder.endObject();

        builder.endObject();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Map);
        Map<?, ?> outerMap = (Map<?, ?>) result;
        assertEquals(2, outerMap.size());
        assertEquals("admin", outerMap.get("user"));

        assertTrue(outerMap.get("details") instanceof Map);
        Map<?, ?> nestedMap = (Map<?, ?>) outerMap.get("details");
        assertEquals(2, nestedMap.size());
        assertEquals(123, nestedMap.get("id"));
        assertEquals("administrator", nestedMap.get("type"));
    }

    @Test
    void deserializeMixedNestedStructures() {
        // Create complex nested structure with both arrays and objects
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder objectBuilder = builder.startObject();

        objectBuilder.appendKey("users");
        VariantArrayBuilder arrayBuilder = objectBuilder.startArray();

        // First user object
        VariantObjectBuilder userBuilder1 = arrayBuilder.startObject();
        userBuilder1.appendKey("name");
        userBuilder1.appendString("Alice");
        userBuilder1.appendKey("age");
        userBuilder1.appendInt(25);
        arrayBuilder.endObject();

        // Second user object
        VariantObjectBuilder userBuilder2 = arrayBuilder.startObject();
        userBuilder2.appendKey("name");
        userBuilder2.appendString("Bob");
        userBuilder2.appendKey("age");
        userBuilder2.appendInt(30);
        arrayBuilder.endObject();

        objectBuilder.endArray();

        objectBuilder.appendKey("metadata");
        VariantObjectBuilder metaBuilder = objectBuilder.startObject();
        metaBuilder.appendKey("count");
        metaBuilder.appendInt(2);
        metaBuilder.appendKey("tags");
        VariantArrayBuilder tagsBuilder = metaBuilder.startArray();
        tagsBuilder.appendString("active");
        tagsBuilder.appendString("verified");
        metaBuilder.endArray();
        objectBuilder.endObject();

        builder.endObject();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Map);
        Map<?, ?> rootMap = (Map<?, ?>) result;
        assertEquals(2, rootMap.size());

        // Check users array
        assertTrue(rootMap.get("users") instanceof List);
        List<?> usersList = (List<?>) rootMap.get("users");
        assertEquals(2, usersList.size());

        Map<?, ?> user1 = (Map<?, ?>) usersList.get(0);
        assertEquals("Alice", user1.get("name"));
        assertEquals(25, user1.get("age"));

        Map<?, ?> user2 = (Map<?, ?>) usersList.get(1);
        assertEquals("Bob", user2.get("name"));
        assertEquals(30, user2.get("age"));

        // Check metadata object
        assertTrue(rootMap.get("metadata") instanceof Map);
        Map<?, ?> metadata = (Map<?, ?>) rootMap.get("metadata");
        assertEquals(2, metadata.get("count"));

        assertTrue(metadata.get("tags") instanceof List);
        List<?> tags = (List<?>) metadata.get("tags");
        assertEquals(2, tags.size());
        assertEquals("active", tags.get(0));
        assertEquals("verified", tags.get(1));
    }

    @Test
    void deserializeArrayDeepNesting() {
        VariantBuilder builder = new VariantBuilder();
        VariantArrayBuilder level1 = builder.startArray();
        level1.appendString("level1");

        VariantArrayBuilder level2 = level1.startArray();
        level2.appendString("level2");

        VariantArrayBuilder level3 = level2.startArray();
        level3.appendString("level3");
        level3.appendInt(123);
        level2.endArray();

        level1.endArray();
        builder.endArray();

        Object result = variantRead.deserialize(builder.build());

        assertTrue(result instanceof List);
        List<?> level1List = (List<?>) result;
        assertEquals(2, level1List.size());
        assertEquals("level1", level1List.get(0));

        List<?> level2List = (List<?>) level1List.get(1);
        assertEquals(2, level2List.size());
        assertEquals("level2", level2List.get(0));

        List<?> level3List = (List<?>) level2List.get(1);
        assertEquals(2, level3List.size());
        assertEquals("level3", level3List.get(0));
        assertEquals(123, level3List.get(1));
    }

    @Test
    void deserializeObjectDeepNesting() {
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder level1 = builder.startObject();
        level1.appendKey("level");
        level1.appendString("1");

        level1.appendKey("nested");
        VariantObjectBuilder level2 = level1.startObject();
        level2.appendKey("level");
        level2.appendString("2");

        level2.appendKey("nested");
        VariantObjectBuilder level3 = level2.startObject();
        level3.appendKey("level");
        level3.appendString("3");
        level3.appendKey("data");
        level3.appendInt(456);
        level2.endObject();

        level1.endObject();
        builder.endObject();

        Object result = variantRead.deserialize(builder.build());

        assertTrue(result instanceof Map);
        Map<?, ?> level1Map = (Map<?, ?>) result;
        assertEquals("1", level1Map.get("level"));

        Map<?, ?> level2Map = (Map<?, ?>) level1Map.get("nested");
        assertEquals("2", level2Map.get("level"));

        Map<?, ?> level3Map = (Map<?, ?>) level2Map.get("nested");
        assertEquals("3", level3Map.get("level"));
        assertEquals(456, level3Map.get("data"));
    }

    @Test
    void deserializeAllTypesInArray() {
        VariantBuilder builder = new VariantBuilder();
        VariantArrayBuilder arrayBuilder = builder.startArray();

        // Add all primitive types
        arrayBuilder.appendNull();
        arrayBuilder.appendBoolean(true);
        arrayBuilder.appendByte((byte) 1);
        arrayBuilder.appendShort((short) 2);
        arrayBuilder.appendInt(3);
        arrayBuilder.appendLong(4L);
        arrayBuilder.appendFloat(5.5f);
        arrayBuilder.appendDouble(6.6);
        arrayBuilder.appendString("seven");
        arrayBuilder.appendDecimal(new BigDecimal("8.8"));
        arrayBuilder.appendUUID(UUID.fromString("00000000-0000-0000-0000-000000000009"));

        // Add temporal types
        arrayBuilder.appendDate(0); // epoch date
        arrayBuilder.appendTime(0L); // midnight
        arrayBuilder.appendTimestampTz(0L); // epoch timestamp
        arrayBuilder.appendTimestampNanosNtz(0L); // epoch timestamp nanos
        arrayBuilder.appendTimestampNtz(0L); // epoch timestamp no tz
        arrayBuilder.appendTimestampNanosTz(0L); // epoch timestamp nanos tz
        arrayBuilder.appendBinary(ByteBuffer.wrap(new byte[] { 17 }));

        builder.endArray();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof List);
        List<?> list = (List<?>) result;
        assertEquals(18, list.size());

        // Verify types
        assertNull(list.get(0));
        assertEquals(true, list.get(1));
        assertEquals((byte) 1, list.get(2));
        assertEquals((short) 2, list.get(3));
        assertEquals(3, list.get(4));
        assertEquals(4L, list.get(5));
        assertEquals(5.5f, (Float) list.get(6), 0.001f);
        assertEquals(6.6, (Double) list.get(7), 0.001);
        assertEquals("seven", list.get(8));
        assertEquals(new BigDecimal("8.8"), list.get(9));
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000009"), list.get(10));
        assertEquals(LocalDate.ofEpochDay(0), list.get(11));
        assertEquals(LocalTime.MIDNIGHT, list.get(12));
        assertEquals(Instant.EPOCH, list.get(13));
        assertTrue(list.get(14) instanceof LocalDateTime);
        assertTrue(list.get(15) instanceof LocalDateTime);
        assertEquals(Instant.EPOCH, list.get(16));
        assertEquals(Binary.fromConstantByteArray(new byte[] { 17 }), list.get(17));
    }

    @Test
    void deserializeAllTypesInObject() {
        // Test object containing all supported primitive types
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder objectBuilder = builder.startObject();

        // Add all primitive types as object fields
        objectBuilder.appendKey("nullValue");
        objectBuilder.appendNull();
        objectBuilder.appendKey("boolValue");
        objectBuilder.appendBoolean(false);
        objectBuilder.appendKey("byteValue");
        objectBuilder.appendByte((byte) 127);
        objectBuilder.appendKey("shortValue");
        objectBuilder.appendShort((short) 32767);
        objectBuilder.appendKey("intValue");
        objectBuilder.appendInt(2147483647);
        objectBuilder.appendKey("longValue");
        objectBuilder.appendLong(9223372036854775807L);
        objectBuilder.appendKey("floatValue");
        objectBuilder.appendFloat(-3.14f);
        objectBuilder.appendKey("doubleValue");
        objectBuilder.appendDouble(-2.718281828);
        objectBuilder.appendKey("stringValue");
        objectBuilder.appendString("test string");
        objectBuilder.appendKey("decimalValue");
        objectBuilder.appendDecimal(new BigDecimal("-999.999"));
        objectBuilder.appendKey("uuidValue");
        objectBuilder.appendUUID(UUID.fromString("12345678-1234-5678-9abc-123456789def"));

        // Binary type is commented because variang bug
        // https://github.com/apache/parquet-java/issues/3315
        // objectBuilder.appendKey("binaryValue");
        // objectBuilder.appendBinary(ByteBuffer.wrap(new byte[] { -1, -2, -3 }));

        // Add temporal types
        objectBuilder.appendKey("dateValue");
        objectBuilder.appendDate(18000); // Some future date
        objectBuilder.appendKey("timeValue");
        objectBuilder.appendTime(43200000000L); // Noon in microseconds
        objectBuilder.appendKey("timestampTzValue");
        objectBuilder.appendTimestampTz(1609459200000000L); // 2021-01-01T00:00:00Z in microseconds
        objectBuilder.appendKey("timestampNanosNtzValue");
        objectBuilder.appendTimestampNanosNtz(1609459200000000000L); // 2021-01-01T00:00:00 in nanoseconds
        objectBuilder.appendKey("timestampNtzValue");
        objectBuilder.appendTimestampNtz(1609459200000000L); // 2021-01-01T00:00:00 in microseconds
        objectBuilder.appendKey("timestampNanosTzValue");
        objectBuilder.appendTimestampNanosTz(1609459200000000000L); // 2021-01-01T00:00:00Z in nanoseconds

        builder.endObject();

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Map);
        Map<?, ?> map = (Map<?, ?>) result;
        assertEquals(18 - 1, map.size());

        // Verify values
        assertNull(map.get("nullValue"));
        assertEquals(false, map.get("boolValue"));
        assertEquals((byte) 127, map.get("byteValue"));
        assertEquals((short) 32767, map.get("shortValue"));
        assertEquals(2147483647, map.get("intValue"));
        assertEquals(9223372036854775807L, map.get("longValue"));
        assertEquals(-3.14f, (Float) map.get("floatValue"), 0.001f);
        assertEquals(-2.718281828, (Double) map.get("doubleValue"), 0.000001);
        assertEquals("test string", map.get("stringValue"));
        assertEquals(new BigDecimal("-999.999"), map.get("decimalValue"));
        assertEquals(UUID.fromString("12345678-1234-5678-9abc-123456789def"), map.get("uuidValue"));
        // assertEquals(Binary.fromConstantByteArray(new byte[] { -1, -2, -3 }),
        // map.get("binaryValue"));
        assertEquals(LocalDate.ofEpochDay(18000), map.get("dateValue"));
        assertEquals(LocalTime.ofNanoOfDay(43200000000L * 1000L), map.get("timeValue"));
        assertTrue(map.get("timestampTzValue") instanceof Instant);
        assertTrue(map.get("timestampNanosNtzValue") instanceof LocalDateTime);
        assertTrue(map.get("timestampNtzValue") instanceof LocalDateTime);
        assertTrue(map.get("timestampNanosTzValue") instanceof Instant);
    }

    @Test
    void deserializeLargeString() {
        // Test deserialization of a large string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("This is a long test string number ").append(i).append(". ");
        }
        String largeString = sb.toString();

        VariantBuilder builder = new VariantBuilder();
        builder.appendString(largeString);

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof String);
        assertEquals(largeString, result);
    }

    @Test
    void deserializeLargeBinaryData() {
        byte[] largeData = new byte[10000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        VariantBuilder builder = new VariantBuilder();
        builder.appendBinary(ByteBuffer.wrap(largeData));

        Object result = variantRead.deserialize(builder.build());
        assertTrue(result instanceof Binary);
        Binary resultBinary = (Binary) result;

        Binary expectedBinary = Binary.fromConstantByteArray(largeData);
        assertEquals(expectedBinary, resultBinary);
    }
}