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

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static com.jerolba.carpet.impl.read.SchemaValidation.hasMapShape;
import static com.jerolba.carpet.impl.read.SchemaValidation.isBasicSupportedType;
import static com.jerolba.carpet.impl.read.SchemaValidation.isThreeLevel;
import static java.util.stream.Collectors.toMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class SchemaFilter {

    private final GroupType schema;
    private final SchemaValidation validation;

    public SchemaFilter(SchemaValidation validation, GroupType schema) {
        this.schema = schema;
        this.validation = validation;
    }

    public MessageType project(Class<?> readClass) {
        ColumnPath path = new ColumnPath();
        GroupType projected = filter(readClass, path);
        return new MessageType(projected.getName(), projected.getFields());
    }

    private GroupType filter(Class<?> readClass, ColumnPath path) {
        if (!readClass.isRecord()) {
            throw new RecordTypeConversionException(readClass.getName() + " is not a Java Record");
        }

        List<Type> fields = schema.getFields();
        Map<String, Type> fieldsByName = fields.stream().collect(toMap(Type::getName, f -> f));

        Map<String, Type> inProjection = new HashMap<>();
        for (RecordComponent recordComponent : readClass.getRecordComponents()) {
            String name = getFieldName(recordComponent);
            ColumnPath column = path.add(readClass, recordComponent.getName(), name);
            Type parquetType = fieldsByName.get(name);
            if (parquetType == null) {
                validation.validateMissingColumn(name, column);
                continue;
            }

            if (parquetType.isRepetition(Repetition.REPEATED)) {
                Type type = analyzeOneLevelStructure(column, recordComponent, parquetType);
                inProjection.put(name, type);
                continue;
            }

            if (parquetType.isPrimitive()) {
                PrimitiveType primitiveType = parquetType.asPrimitiveType();
                validation.validatePrimitiveCompatibility(primitiveType, recordComponent.getType());
                validation.validateNullability(primitiveType, recordComponent);
                inProjection.put(name, parquetType);
                continue;
            }
            GroupType asGroupType = parquetType.asGroupType();
            LogicalTypeAnnotation typeAnnotation = parquetType.getLogicalTypeAnnotation();
            if (typeAnnotation == listType()) {
                if (!Collection.class.isAssignableFrom(recordComponent.getType())) {
                    throw new RecordTypeConversionException("Field '" + name + "' is not a collection in '"
                            + column.getClassName() + "' mapping column '" + column.path() + "'");
                }
                var parameterized = getParameterizedCollection(recordComponent);
                Type type = analyzeMultipleLevelStructure(column, name, parameterized, asGroupType);
                inProjection.put(name, type);
                continue;
            } else if (typeAnnotation == mapType()) {
                if (!Map.class.isAssignableFrom(recordComponent.getType())) {
                    throw new RecordTypeConversionException("Field '" + name + "' is not a map in '"
                            + column.getClassName() + "' mapping column '" + column.path() + "'");
                }
                var parameterized = getParameterizedMap(recordComponent);
                Type type = analizeMapStructure(column, name, parameterized, asGroupType);
                inProjection.put(name, type);
                continue;
            }

            if (recordComponent.getType().isRecord()) {
                validation.validateNullability(parquetType, recordComponent);
                SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);

                GroupType recordSchema = recordFilter.filter(recordComponent.getType(), column);
                inProjection.put(name, recordSchema);
            } else {
                throw new RecordTypeConversionException(recordComponent.getType().getName() + " is not a Java Record");
            }
        }
        List<Type> projection = schema.getFields().stream()
                .filter(f -> inProjection.containsKey(f.getName()))
                .map(f -> inProjection.get(f.getName()))
                .toList();
        return new GroupType(schema.getRepetition(), schema.getName(), projection);
    }

    private Type analyzeOneLevelStructure(ColumnPath column, RecordComponent recordComponent,
            Type parquetType) {

        // Java field must be a collection type
        if (!Collection.class.isAssignableFrom(recordComponent.getType())) {
            throw new RecordTypeConversionException("Repeated field " + recordComponent.getName() + " of "
                    + column.getClassName() + " is not a collection");
        }
        var parameterized = getParameterizedCollection(recordComponent);
        if (parameterized.isCollection()) {
            // Is Java child recursive collection or map?
            throw new RecordTypeConversionException(
                    "1-level collections can no embed nested collections (List<List<?>>)");
        }
        if (parameterized.isMap()) {
            var parameterizedChild = parameterized.getParametizedAsMap();
            return analizeMapStructure(column, parquetType.getName(), parameterizedChild, parquetType.asGroupType());
        }
        if (parquetType.isPrimitive()) {
            // if collection type is Java "primitive"
            var primitiveType = parquetType.asPrimitiveType();
            var actualCollectionType = parameterized.getActualType();
            validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
            return parquetType;
        }
        // if collection type is Java "Record"
        var asGroupType = parquetType.asGroupType();
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, asGroupType);
            return recordFilter.filter(actualCollectionType, column);
        }
        throw new RecordTypeConversionException("Field " + getFieldName(recordComponent) + " of type "
                + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
    }

    private Type analyzeMultipleLevelStructure(ColumnPath column, String name, ParameterizedCollection parameterized,
            GroupType groupType) {
        if (groupType.getFieldCount() > 1) {
            throw new RecordTypeConversionException("Nestd list " + groupType.getName() + " must have only one item");
        }
        Type groupChild = groupType.getFields().get(0);
        if (!groupChild.isRepetition(Repetition.REPEATED)) {
            throw new RecordTypeConversionException("Nestd list element " + groupChild.getName() + " must be REPEATED");
        }
        if (isThreeLevel(groupChild)) {
            GroupType listGroup = groupChild.asGroupType();
            Type childGroupChild = listGroup.getFields().get(0);
            return analyzeListLevelStructure(column, name, parameterized, groupType, listGroup, childGroupChild);
        }
        return analyzeListLevelStructure(column, name, parameterized, groupType, null, groupChild);
    }

    private Type analyzeListLevelStructure(ColumnPath column, String name, ParameterizedCollection parameterized,
            GroupType parentGroupType, GroupType listGroup, Type childElement) {

        if (parameterized.isCollection() || parameterized.isMap()) {
            LogicalTypeAnnotation typeAnnotation = childElement.getLogicalTypeAnnotation();
            if (typeAnnotation == listType()) {
                if (!parameterized.isCollection()) {
                    throw new RecordTypeConversionException("Field " + name + " of " + column.getClassName()
                            + " is not a collection");
                }
                var parameterizedChild = parameterized.getParametizedAsCollection();
                Type type = analyzeMultipleLevelStructure(column, name, parameterizedChild, childElement.asGroupType());
                Type filtered = rewrapListIfExists(listGroup, type);
                return parentGroupType.withNewFields(filtered);
            } else if (typeAnnotation == mapType()) {
                if (!parameterized.isMap()) {
                    throw new RecordTypeConversionException("Field " + name + " of " + column.getClassName()
                            + " is not a Map");
                }
                var parameterizedChild = parameterized.getParametizedAsMap();
                Type type = analizeMapStructure(column, name, parameterizedChild, childElement.asGroupType());
                Type filtered = rewrapListIfExists(listGroup, type);
                return parentGroupType.withNewFields(filtered);
            }
            throw new RecordTypeConversionException("Field " + name + " of " + column.getClassName()
                    + " is not a collection");
        }
        if (childElement.isPrimitive()) {
            var primitiveType = childElement.asPrimitiveType();
            var actualCollectionType = parameterized.getActualType();
            validation.validatePrimitiveCompatibility(primitiveType, actualCollectionType);
            return parentGroupType;
        }
        var actualCollectionType = parameterized.getActualType();
        if (actualCollectionType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, childElement.asGroupType());
            GroupType childMapped = recordFilter.filter(actualCollectionType, column);
            Type listGroupMapped = rewrapListIfExists(listGroup, childMapped);
            return parentGroupType.withNewFields(listGroupMapped);
        }
        if (isBasicSupportedType(new JavaType(actualCollectionType)) && !childElement.isPrimitive()) {
            throw new RecordTypeConversionException(
                    childElement.getName() + " is not compatible with " + actualCollectionType.getName());
        }
        throw new RecordTypeConversionException("Field " + name + " of type "
                + actualCollectionType.getName() + " is not a basic type or " + "a Java record");
    }

    private Type rewrapListIfExists(GroupType listGroupRepeated, Type type) {
        if (listGroupRepeated == null) {
            return type;
        }
        return listGroupRepeated.withNewFields(type);
    }

    private Type analizeMapStructure(ColumnPath column, String name, ParameterizedMap parameterized,
            GroupType mapType) {
        if (!hasMapShape(mapType)) {
            throw new RecordTypeConversionException("Field " + mapType.getName() + " is not a valid map");
        }
        GroupType keyValueType = mapType.getFields().get(0).asGroupType();

        // Review Key
        Type key = keyValueType.getFields().get(0);
        if (parameterized.keyIsCollection() || parameterized.keyIsMap()) {
            throw new RecordTypeConversionException("Maps and Collections can not be key of a Map");
        }
        Class<?> keyActualType = parameterized.getKeyActualType();
        if (key.isPrimitive()) {
            PrimitiveType primitiveType = key.asPrimitiveType();
            validation.validatePrimitiveCompatibility(primitiveType, keyActualType);
        } else if (keyActualType.isRecord()) {
            SchemaFilter recordFilter = new SchemaFilter(validation, key.asGroupType());
            key = recordFilter.filter(keyActualType, column);
        } else {
            throw new RecordTypeConversionException(keyActualType.getName() + " is not a valid key for a Map");
        }

        // Review value
        Type value = keyValueType.getFields().get(1);
        if (parameterized.valueIsCollection() || parameterized.valueIsMap()) {
            LogicalTypeAnnotation typeAnnotation = value.getLogicalTypeAnnotation();
            if (listType().equals(typeAnnotation)) {
                if (!parameterized.valueIsCollection()) {
                    throw new RecordTypeConversionException("Field " + name + " of " + column.getClassName()
                            + " is not a collection");
                }
                var parameterizedChild = parameterized.getValueTypeAsCollection();
                value = analyzeMultipleLevelStructure(column, name, parameterizedChild, value.asGroupType());
            } else if (mapType().equals(typeAnnotation)) {
                if (!parameterized.valueIsMap()) {
                    throw new RecordTypeConversionException("Field " + name + " of " + column.getClassName()
                            + " is not a map");
                }
                var parameterizedChild = parameterized.getValueTypeAsMap();
                value = analizeMapStructure(column, name, parameterizedChild, value.asGroupType());
            }
        } else {
            Class<?> valueActualType = parameterized.getValueActualType();
            if (value.isPrimitive()) {
                validation.validatePrimitiveCompatibility(value.asPrimitiveType(), valueActualType);
            } else if (valueActualType.isRecord()) {
                SchemaFilter recordFilter = new SchemaFilter(validation, value.asGroupType());
                value = recordFilter.filter(valueActualType, column);
            } else {
                throw new RecordTypeConversionException(valueActualType.getName() + " is not a valid key for a Map");
            }
        }
        Type keyValueRebuild = keyValueType.withNewFields(key, value);
        return mapType.withNewFields(keyValueRebuild);
    }

}
