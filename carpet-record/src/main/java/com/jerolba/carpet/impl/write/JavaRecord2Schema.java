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

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class JavaRecord2Schema {

    private final CarpetWriteConfiguration carpetConfiguration;

    public JavaRecord2Schema(CarpetWriteConfiguration carpetConfiguration) {
        this.carpetConfiguration = carpetConfiguration;
    }

    public MessageType createSchema(Class<?> recordClass) {
        return build(recordClass, new HashSet<>());
    }

    private MessageType build(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        String groupName = recordClass.getSimpleName();

        List<Type> fields = createGroupFields(recordClass, visited);
        return new MessageType(groupName, fields);
    }

    private List<Type> createGroupFields(Class<?> recordClass, Set<Class<?>> visited) {
        List<Type> fields = new ArrayList<>();
        for (var attr : recordClass.getRecordComponents()) {
            Class<?> type = attr.getType();
            String fieldName = getFieldName(attr);
            boolean notNull = type.isPrimitive() || isNotNull(attr);
            Repetition repetition = notNull ? REQUIRED : OPTIONAL;

            PrimitiveTypeName primitiveType = simpleTypeItems(type);
            if (primitiveType != null) {
                fields.add(new PrimitiveType(repetition, primitiveType, fieldName));
            } else if (type.getName().equals("java.lang.String")) {
                fields.add(Types.primitive(BINARY, repetition).as(stringType()).named(fieldName));
            } else if (type.isRecord()) {
                List<Type> childFields = buildCompositeChild(type, visited);
                fields.add(new GroupType(repetition, fieldName, childFields));
            } else if (type.isEnum()) {
                fields.add(Types.primitive(BINARY, repetition).as(enumType()).named(fieldName));
            } else if (type.getName().equals("java.util.UUID")) {
                fields.add(Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(uuidType())
                        .length(UUIDLogicalTypeAnnotation.BYTES).named(fieldName));
            } else if (Collection.class.isAssignableFrom(type)) {
                var parameterizedCollection = getParameterizedCollection(attr);
                fields.add(createCollectionType(fieldName, parameterizedCollection, visited, attr, repetition));
            } else if (Map.class.isAssignableFrom(type)) {
                var parameterizedMap = getParameterizedMap(attr);
                fields.add(createMapType(fieldName, parameterizedMap, visited, repetition));
            } else {
                java.lang.reflect.Type genericType = attr.getGenericType();
                if (genericType instanceof TypeVariable<?>) {
                    throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
                }
                throw new RecordTypeConversionException(
                        "Field '" + attr.getName() + "' of type " + attr.getType() + " not supported");
            }
        }
        return fields;
    }

    private List<Type> buildCompositeChild(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        List<Type> fields = createGroupFields(recordClass, visited);
        visited.remove(recordClass);
        return fields;
    }

    private Type createCollectionType(String fieldName, ParameterizedCollection collectionClass, Set<Class<?>> visited,
            RecordComponent attr, Repetition repetition) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(fieldName, collectionClass, visited);
        case TWO -> createCollectionTwoLevel(fieldName, collectionClass, visited, repetition);
        case THREE -> createCollectionThreeLevel(fieldName, collectionClass, visited, repetition);
        };
    }

    private Type createCollectionOneLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        if (parametized.isMap()) {
            return createMapType(fieldName, parametized.getParametizedAsMap(), visited, REPEATED);
        }
        return buildTypeElement(parametized.getActualType(), visited, REPEATED, fieldName);
    }

    private Type createCollectionTwoLevel(String fieldName, ParameterizedCollection parametized, Set<Class<?>> visited,
            Repetition repetition) {
        // Two level collections elements are not nullables
        Type nested = createNestedCollection(parametized, visited, REPEATED);
        return ConversionPatterns.listType(repetition, fieldName, nested);
    }

    private Type createCollectionThreeLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited, Repetition repetition) {
        // Three level collections elements are nullables
        Type nested = createNestedCollection(parametized, visited, OPTIONAL);
        return ConversionPatterns.listOfElements(repetition, fieldName, nested);
    }

    private Type createNestedCollection(ParameterizedCollection parametized, Set<Class<?>> visited,
            Repetition repetition) {
        if (parametized.isCollection()) {
            return createCollectionType("element", parametized.getParametizedAsCollection(), visited, null, repetition);
        }
        if (parametized.isMap()) {
            return createMapType("element", parametized.getParametizedAsMap(), visited, repetition);
        }
        return buildTypeElement(parametized.getActualType(), visited, repetition, "element");
    }

    private Type createMapType(String fieldName, ParameterizedMap parametized, Set<Class<?>> visited,
            Repetition repetition) {
        Class<?> keyType = parametized.getKeyActualType();
        Type nestedKey = buildTypeElement(keyType, visited, REQUIRED, "key");

        if (parametized.valueIsCollection()) {
            Type childCollection = createCollectionType("value", parametized.getValueTypeAsCollection(),
                    visited, null, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childCollection).named(fieldName);
        }
        if (parametized.valueIsMap()) {
            Type childMap = createMapType("value", parametized.getValueTypeAsMap(), visited, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childMap).named(fieldName);
        }

        Class<?> valueType = parametized.getValueActualType();
        Type nestedValue = buildTypeElement(valueType, visited, OPTIONAL, "value");
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(fieldName);
            // return ConversionPatterns.mapType(repetition, fieldName, "key_value",
            // nestedKey, nestedValue);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Type buildTypeElement(Class<?> type, Set<Class<?>> visited, Repetition repetition, String name) {
        PrimitiveTypeName primitiveKeyType = simpleTypeItems(type);
        if (primitiveKeyType != null) {
            return new PrimitiveType(repetition, primitiveKeyType, name);
        } else if (type.getName().equals("java.lang.String")) {
            return Types.primitive(BINARY, repetition).as(stringType()).named(name);
        } else if (type.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            return new GroupType(repetition, name, childFields);
        } else if (type.isEnum()) {
            return Types.primitive(BINARY, repetition).as(enumType()).named(name);
        } else if (type.getName().equals("java.util.UUID")) {
            return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES)
                    .named(name);
        } else {
            // Generic types in first child are detected
        }
        throw new RecordTypeConversionException("Unsuported type " + type);
    }

    private PrimitiveTypeName simpleTypeItems(Class<?> type) {
        return switch (type.getName()) {
        case "short", "java.lang.Short", "int", "java.lang.Integer" -> PrimitiveTypeName.INT32;
        case "byte", "java.lang.Byte" -> PrimitiveTypeName.INT32;
        case "long", "java.lang.Long" -> PrimitiveTypeName.INT64;
        case "float", "java.lang.Float" -> PrimitiveTypeName.FLOAT;
        case "double", "java.lang.Double" -> PrimitiveTypeName.DOUBLE;
        case "boolean", "java.lang.Boolean" -> PrimitiveTypeName.BOOLEAN;
        default -> null;
        };
    }

    private void validateNotVisitedRecord(Class<?> recordClass, Set<Class<?>> visited) {
        if (!recordClass.isRecord()) {
            throw new RecordTypeConversionException(recordClass.getName() + " must be a java Record");
        }
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive records are not supported");
        }
        visited.add(recordClass);
    }

}
