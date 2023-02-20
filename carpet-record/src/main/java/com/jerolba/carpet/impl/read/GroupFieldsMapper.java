package com.jerolba.carpet.impl.read;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

import com.jerolba.carpet.impl.AliasField;

class GroupFieldsMapper {

    private final Class<?> recordClass;
    private final Map<String, Integer> fieldIndex = new HashMap<>();
    private final Map<String, RecordComponent> fieldType = new HashMap<>();

    public GroupFieldsMapper(Class<?> recordClass) {
        this.recordClass = recordClass;
        RecordComponent[] components = recordClass.getRecordComponents();
        int cont = 0;
        for (RecordComponent recordComponent : components) {
            String name = AliasField.getFieldName(recordComponent);
            fieldIndex.put(name, cont);
            fieldType.put(name, recordComponent);
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