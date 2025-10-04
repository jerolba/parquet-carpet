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
 * Annotation to indicate that a field should be stored as an enumeration type
 * in Parquet format.
 * <p>
 * This annotation can be applied to specify that the annotated String field
 * should be serialized using the ENUM logical type in Parquet. This is
 * particularly useful for String fields that represent a finite set of
 * predefined values.
 * </p>
 * <p>
 * When a field is annotated with {@code @ParquetEnum}, the Carpet library will:
 * <ul>
 * <li>Store the field data using Parquet's ENUM logical type annotation</li>
 * <li>Optimize storage by treating the values as a controlled vocabulary</li>
 * <li>Enable better compression and query performance for categorical data</li>
 * </ul>
 * </p>
 *
 * <h3>Usage Example:</h3>
 *
 * <pre>{@code
 * record User(String name, @ParquetEnum String category) {
 * }
 * }</pre>
 *
 * @see org.apache.parquet.schema.LogicalTypeAnnotation#enumType()
 */
@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface ParquetEnum {

}
