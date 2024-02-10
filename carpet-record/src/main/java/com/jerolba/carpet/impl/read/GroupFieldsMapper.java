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

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.schema.GroupType;

import com.jerolba.carpet.impl.read.ColumnToFieldMapper.NameMap;

class GroupFieldsMapper {

    private final Class<?> recordClass;
    private final Map<String, Integer> fieldIndex = new HashMap<>();
    private final Map<String, RecordComponent> fieldType = new HashMap<>();

    GroupFieldsMapper(GroupType schema, Class<?> recordClass, ColumnToFieldMapper columnToFieldMapper) {
        this.recordClass = recordClass;
        RecordComponent[] components = recordClass.getRecordComponents();
        Map<String, NameMap> mapFields = columnToFieldMapper.mapFields(schema, components);
        int cont = 0;
        for (RecordComponent recordComponent : components) {
            NameMap mapping = mapFields.get(recordComponent.getName());
            if (mapping != null) {
                String name = mapping.parquetType().getName();
                fieldIndex.put(name, cont);
                fieldType.put(name, recordComponent);
            }
            cont++;
        }
    }

    public int getIndex(String name) {
        Integer idx = fieldIndex.get(name);
        if (idx == null) {
            throw new RuntimeException("Field " + name + " not present in class " + recordClass);
        }
        return idx;
    }

    public RecordComponent getRecordComponent(String name) {
        var rc = fieldType.get(name);
        if (rc == null) {
            throw new RuntimeException("Field " + name + " not present in class " + recordClass);
        }
        return rc;
    }

}