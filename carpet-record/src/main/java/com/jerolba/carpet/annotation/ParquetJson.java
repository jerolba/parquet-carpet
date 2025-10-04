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
 * Annotation to indicate that a field should be stored as JSON in Parquet
 * format.
 * <p>
 * This annotation can be applied to specify that the annotated field should be
 * serialized using the JSON logical type in Parquet. This is useful for storing
 * complex nested structures, dynamic schemas, or any data that is naturally
 * represented as JSON.
 * </p>
 * <p>
 * The annotated field must be a String or a Binary type that contains valid
 * JSON data. The annotation will not serialize the field to JSON; it only
 * indicates how the field should be treated when writing to or reading from
 * Parquet files.
 * </p>
 *
 * <h3>Usage Examples:</h3>
 *
 * <pre>
 * {
 *     &#64;code
 *     record EventData(String eventId,
 *             &#64;ParquetJson String payload,
 *             &#64;ParquetJson Binary metadata) {
 *     }
 * }
 * </pre>
 *
 * @see org.apache.parquet.schema.LogicalTypeAnnotation#jsonType()
 */
@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface ParquetJson {

}
