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
 * Annotation to explicitly indicate that a field should be stored as a string
 * type in Parquet format.
 * <p>
 * This annotation can be applied to specify that an Enum of Binary annotated
 * field should be serialized using the STRING logical type in Parquet.
 * </p>
 *
 * <h3>Usage Examples:</h3>
 *
 * <pre>
 * {
 *     &#64;code
 *     record UserRecord(
 *             long id,
 *             &#64;ParquetString Binary name,
 *             &#64;CategoryEnum category,
 *             String email // automatically treated as string without annotation
 *     ) {
 *     }
 * }
 * </pre>
 *
 * <p>
 * <b>Note:</b> This annotation is primarily used for explicit schema definition
 * and documentation purposes, as string fields are typically handled
 * automatically by the Carpet library. It can be particularly useful when
 * working with schema evolution or when integrating with other Parquet tools
 * that require explicit type annotations.
 * </p>
 *
 * @see org.apache.parquet.schema.LogicalTypeAnnotation#stringType()
 */
@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface ParquetString {

}
