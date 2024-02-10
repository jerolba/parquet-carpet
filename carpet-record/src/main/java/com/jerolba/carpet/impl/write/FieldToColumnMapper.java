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
package com.jerolba.carpet.impl.write;

import static com.jerolba.carpet.impl.CaseConverter.camelCaseToSnakeCase;

import java.lang.reflect.RecordComponent;

import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.annotation.Alias;

class FieldToColumnMapper {

    private final ColumnNamingStrategy columnNamingStrategy;

    public FieldToColumnMapper(ColumnNamingStrategy columnNamingStrategy) {
        this.columnNamingStrategy = columnNamingStrategy;
    }

    public String getColumnName(RecordComponent recordComponent) {
        Alias annotation = recordComponent.getAnnotation(Alias.class);
        if (annotation != null) {
            return annotation.value();
        }
        String name = recordComponent.getName();
        return switch (columnNamingStrategy) {
        case FIELD_NAME -> name;
        case SNAKE_CASE -> camelCaseToSnakeCase(name);
        };
    }

}
