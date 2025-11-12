/**
 * Copyright 2025 Jerónimo López Bezanilla
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
 * <p><b>Important:</b> Field IDs must be unique within the same record (including nested records).
 * It is the developer's responsibility to ensure uniqueness. Duplicate field IDs within the same
 * record will result in undefined behavior when reading or writing Parquet files.</p>
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
@Target(RECORD_COMPONENT)
public @interface FieldId {

    /**
     * The unique field ID for this field within the Parquet schema.
     * <p>
     * Field IDs must be:
     * <ul>
     * <li>Positive integers</li>
     * <li>Unique within the same record scope (sibling fields must have different IDs)</li>
     * </ul>
     * <p>
     * Note: Carpet does not validate field ID uniqueness. It is the developer's responsibility
     * to ensure IDs are unique within each record.
     *
     * @return the field ID
     */
    int value();

}
