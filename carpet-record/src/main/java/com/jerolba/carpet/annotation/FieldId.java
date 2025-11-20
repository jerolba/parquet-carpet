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
package com.jerolba.carpet.annotation;

import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Specifies the Parquet field ID for a record component.
 * Field IDs are critical for schema evolution in Parquet files, particularly when:
 * <ul>
 * <li>Column names or order change over time</li>
 * <li>Files need to maintain backward/forward compatibility with different schema versions</li>
 * <li>Integrating with systems that rely on field IDs for column resolution</li>
 * <li>Working with data catalogs that use field IDs for schema management</li>
 * </ul>
 *
 * <p><b>Important Guidelines:</b></p>
 * <ul>
 * <li><b>Uniqueness:</b> Field IDs must be unique within the same record scope. Sibling fields
 * (fields at the same level within a record) must have different field IDs. Duplicate field IDs
 * within the same record scope will cause a {@link com.jerolba.carpet.RecordTypeConversionException}
 * to be thrown during schema creation.</li>
 * <li><b>Reuse across scopes:</b> Field IDs can be reused across different record scopes
 * (e.g., different nested records).</li>
 * <li><b>Stability:</b> Once assigned, field IDs should never change for a given field.
 * Maintain the same IDs across all schema versions to ensure proper schema evolution.</li>
 * <li><b>No recycling:</b> Never reuse a field ID from a deleted field for a different field.
 * This prevents misinterpretation by ID-aware readers.</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>
 * record SomeRecord(
 *     &#64;FieldId(1) String uuid,
 *     &#64;FieldId(2) int statusCode,
 *     &#64;FieldId(3) long durationMillis,
 *     &#64;FieldId(4) String error
 * ) {}
 * </pre>
 *
 * <p>For nested records, field IDs must be unique within each record scope:</p>
 * <pre>
 * record Address(
 *     &#64;FieldId(100) String street,
 *     &#64;FieldId(101) String city
 * ) {}
 *
 * record Person(
 *     &#64;FieldId(1) String name,
 *     &#64;FieldId(2) Address address
 * ) {}
 * </pre>
 */
@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface FieldId {

    /**
     * The unique field ID for this field within the Parquet schema.
     * <p>
     * Field IDs must be:
     * <ul>
     * <li>Positive integers</li>
     * <li>Unique within the same record scope (sibling fields must have different IDs)</li>
     * <li>Stable across all schema versions - never change an ID once assigned</li>
     * <li>Never reused for different fields, even after a field is removed</li>
     * </ul>
     * <p>
     * Carpet validates field ID uniqueness at schema creation time and will throw a
     * {@link com.jerolba.carpet.RecordTypeConversionException} if duplicate field IDs
     * are detected within the same record scope.
     * <p>
     * <b>Note:</b> Field IDs are applied to record component fields. Internal Parquet structures
     * (such as list elements or map key/value containers) do not receive field IDs from annotations,
     * as these are managed by the library according to Parquet's standard encoding conventions.
     *
     * @return the field ID
     */
    int value();

}
