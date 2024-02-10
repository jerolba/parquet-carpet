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
package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.FieldMatchingStrategy.BEST_EFFORT;
import static com.jerolba.carpet.FieldMatchingStrategy.FIELD_NAME;
import static com.jerolba.carpet.FieldMatchingStrategy.SNAKE_CASE;
import static com.jerolba.carpet.impl.CaseConverter.camelCaseToSnakeCase;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.FieldMatchingStrategy;
import com.jerolba.carpet.impl.AliasField;

public class ColumnToFieldMapper {

    public record NameMap(RecordComponent recordComponent, Type parquetType) {
    }

    private final FieldMatchingStrategy fieldMatchingStrategy;

    public ColumnToFieldMapper(FieldMatchingStrategy fieldMatchingStrategy) {
        this.fieldMatchingStrategy = fieldMatchingStrategy;
    }

    public Map<String, NameMap> mapFields(GroupType schema, RecordComponent[] recordComponent) {
        var mapper = new RecordMapper(recordComponent, schema);
        mapper.mapWith(AliasField::getComponentAlias);
        if (fieldMatchingStrategy == FIELD_NAME || fieldMatchingStrategy == BEST_EFFORT) {
            mapper.mapWith(RecordComponent::getName);
        }
        if (fieldMatchingStrategy == SNAKE_CASE || fieldMatchingStrategy == BEST_EFFORT) {
            mapper.mapWith(component -> camelCaseToSnakeCase(component.getName()));
        }
        return mapper.getResult();
    }

    private class RecordMapper {

        private final Map<String, NameMap> result = new HashMap<>();
        private final RecordComponent[] recordComponents;
        private final Map<String, Type> parquetIndexed;

        public RecordMapper(RecordComponent[] recordComponents, GroupType schema) {
            this.recordComponents = recordComponents;
            this.parquetIndexed = schema.getFields().stream().collect(toMap(Type::getName, f -> f));
        }

        public void mapWith(Function<RecordComponent, String> mapper) {
            for (var component : recordComponents) {
                if (!result.containsKey(component.getName())) {
                    String toTest = mapper.apply(component);
                    Type type = parquetIndexed.get(toTest);
                    if (type != null) {
                        result.put(component.getName(), new NameMap(component, type));
                    }
                }
            }
        }

        public Map<String, NameMap> getResult() {
            return result;
        }

    }

}
