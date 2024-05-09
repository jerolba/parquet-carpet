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
package com.jerolba.carpet;

import static com.jerolba.carpet.impl.read.SchemaValidation.isThreeLevel;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.io.FileSystemInputFile;

/**
 * Experimental utility class. The API and behavior can change in the future.
 *
 * Given a Parquet file generates Java source code needed to parse the file.
 * <ul>
 * <li>Each element of List is a Record</li>
 * <li>Generates all possible Records</li>
 * <li>Nested classes use as Record name the name of the field that references
 * them</li>
 * <li>Enums are declared as String</li>
 * <li>If two records have the same fields of the same type, the first to appear
 * is selected</li>
 * </ul>
 *
 */
public class CarpetRecordGenerator {

    public static List<String> generateCode(String filePath) throws IOException {
        return generateCode(new File(filePath));
    }

    public static List<String> generateCode(File file) throws IOException {
        InputFile inputFile = new FileSystemInputFile(file);
        return generateCode(inputFile);
    }

    public static List<String> generateCode(InputFile inputFile) throws IOException {
        return new RecordCodeBuilder().calculate(inputFile);
    }

    private static class RecordCodeBuilder {

        private List<String> calculate(InputFile inputFile) throws IOException {
            ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
            MessageType schema = fileReader.getFileMetaData().getSchema();
            SchemaInspector inspector = new SchemaInspector();
            RecordMetadata generated = inspector.inspectGroup(schema, schema.getName()).recordClass();
            Set<RecordMetadata> allClasses = new TreeTraversal().locateClassesInTree(generated);
            CodeGenerator codeGenerator = new CodeGenerator();
            return allClasses.stream().map(codeGenerator::recordToString).toList();
        }

        private static class TreeTraversal {

            private Set<RecordMetadata> locateClassesInTree(RecordMetadata generated) {
                Set<RecordMetadata> lst = new LinkedHashSet<>();
                List<RecordField> fields = generated.fields;
                for (RecordField field : fields) {
                    lst.addAll(locateRecords(field.fieldType));
                }
                lst.add(generated);
                return lst;
            }

            private Set<RecordMetadata> locateRecords(FieldType type) {
                if (type instanceof RecordType r) {
                    return locateClassesInTree(r.recordClass);
                }
                if (type instanceof ListType rec) {
                    return locateRecords(rec.listType);
                }
                if (type instanceof MapType map) {
                    Set<RecordMetadata> mapClasses = new LinkedHashSet<>();
                    mapClasses.addAll(locateRecords(map.keyType));
                    mapClasses.addAll(locateRecords(map.valueType));
                    return mapClasses;
                }
                return Set.of();
            }

        }

        private static class CodeGenerator {

            private String recordToString(RecordMetadata generated) {
                List<RecordField> fields = generated.fields;
                String value = "record " + generated.recorddName + "(";
                boolean first = true;
                for (RecordField field : fields) {
                    var type = field.fieldType;
                    if (!first) {
                        value += ", ";
                    }
                    value += type.getJavaType() + " " + field.name;
                    first = false;
                }
                value += ") {}";
                return value;
            }

        }

        private static class SchemaInspector {

            private final Map<Set<Type>, RecordType> existingRecords = new HashMap<>();

            private RecordType inspectGroup(GroupType groupType, String inheritedName) {
                Set<Type> fieldsSet = new HashSet<>(groupType.getFields());
                if (existingRecords.containsKey(fieldsSet)) {
                    return existingRecords.get(fieldsSet);
                }

                RecordMetadata current = new RecordMetadata(groupType, inheritedName);
                for (var schemaField : groupType.getFields()) {
                    FieldType fieldType = buildField(schemaField, schemaField.getName());
                    if (schemaField.isRepetition(Repetition.REPEATED)) {
                        fieldType = new ListType(fieldType);
                    }
                    current.fields().add(new RecordField(fieldType, schemaField));
                }
                RecordType recordType = new RecordType(current);
                existingRecords.put(fieldsSet, recordType);
                return recordType;
            }

            private FieldType buildField(Type field, String fieldName) {
                if (field.isPrimitive()) {
                    return PrimitiveFieldFactory.buildRecordField(field);
                }
                GroupType asGroupType = field.asGroupType();
                LogicalTypeAnnotation logicalType = asGroupType.getLogicalTypeAnnotation();
                if (listType().equals(logicalType)) {
                    return inspectListField(asGroupType, fieldName);
                } else if (mapType().equals(logicalType)) {
                    return inspectMapField(asGroupType, fieldName);
                }
                return inspectGroup(field.asGroupType(), fieldName);
            }

            private FieldType inspectListField(GroupType listField, String listFieldName) {
                Type listChild = listField.getFields().get(0);
                Type listElement = isThreeLevel(listChild) ? listChild.asGroupType().getFields().get(0) : listChild;
                return new ListType(buildField(listElement, listFieldName));
            }

            private FieldType inspectMapField(GroupType mapField, String mapFieldName) {
                List<Type> fields = mapField.getFields();
                if (fields.size() > 1) {
                    throw new RecordTypeConversionException(
                            mapField.getName() + " MAP can not have more than one field");
                }
                GroupType mapChild = fields.get(0).asGroupType();
                List<Type> mapFields = mapChild.getFields();
                if (mapFields.size() != 2) {
                    throw new RecordTypeConversionException(
                            mapField.getName() + " MAP child element must have two fields");
                }
                FieldType keyType = buildField(mapFields.get(0), mapFieldName + "Key");
                FieldType valueType = buildField(mapFields.get(1), mapFieldName);
                return new MapType(keyType, valueType);
            }

        }
    }

    private static record RecordMetadata(GroupType groupType, String recorddName, List<RecordField> fields) {

        public RecordMetadata(GroupType groupType, String inheritedName) {
            this(groupType, extractClassName(inheritedName), new ArrayList<>());
        }

    }

    private sealed interface FieldType permits BasicType, ListType, MapType, RecordType {
        String getJavaType();
    }

    private record BasicType(BasicTypes type, boolean notNull) implements FieldType {

        @Override
        public String getJavaType() {
            return notNull ? type.primitive : type.object;
        }

    }

    private record ListType(FieldType listType) implements FieldType {

        @Override
        public String getJavaType() {
            return "List<" + listType.getJavaType() + ">";
        }

    }

    private record MapType(FieldType keyType, FieldType valueType) implements FieldType {

        @Override
        public String getJavaType() {
            return "Map<" + keyType.getJavaType() + ", " + valueType.getJavaType() + ">";
        }

    }

    private record RecordType(RecordMetadata recordClass) implements FieldType {

        @Override
        public String getJavaType() {
            return recordClass.recorddName;
        }

    }

    private record RecordField(FieldType fieldType, String name) {

        public RecordField(FieldType type, Type parquetField) {
            this(type, parquetField.getName());
        }

    }

    private enum BasicTypes {
        BYTE_TYPE("byte", "Byte"),
        SHORT_TYPE("short", "Short"),
        INT_TYPE("int", "Integer"),
        LONG_TYPE("long", "Long"),
        FLOAT_TYPE("float", "Float"),
        DOUBLE_TYPE("double", "Double"),
        BOOLEAN_TYPE("boolean", "Boolean"),
        STRING_TYPE("String"),
        ENUM_TYPE("String"),
        UUID_TYPE("UUID"),
        LOCAL_DATE_TYPE("LocalDate"),
        LOCAL_TIME_TYPE("LocalTime"),
        LOCAL_DATE_TIME_TYPE("LocalDateTime"),
        INSTANT_TYPE("Instant"),
        DECIMAL_TYPE("BigDecimal");

        private final String primitive;
        private final String object;

        private BasicTypes(String primitive, String object) {
            this.primitive = primitive;
            this.object = object;
        }

        private BasicTypes(String object) {
            this(object, object);
        }
    }

    private static class PrimitiveFieldFactory {

        static FieldType buildRecordField(Type parquetField) {
            PrimitiveTypeName typeName = parquetField.asPrimitiveType().getPrimitiveTypeName();
            BasicTypes basicType = buildFromLogicalType(parquetField);
            if (basicType == null) {
                basicType = switch (typeName) {
                case INT32 -> BasicTypes.INT_TYPE;
                case INT64 -> BasicTypes.LONG_TYPE;
                case FLOAT -> BasicTypes.FLOAT_TYPE;
                case DOUBLE -> BasicTypes.DOUBLE_TYPE;
                case BOOLEAN -> BasicTypes.BOOLEAN_TYPE;
                default -> throw new RecordTypeConversionException(typeName + " deserialization not supported");
                };
            }

            return new BasicType(basicType, parquetField.isRepetition(Repetition.REQUIRED));
        }

        public static BasicTypes buildFromLogicalType(Type parquetField) {
            var logicalTypeAnnotation = parquetField.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                return null;
            }
            var primitiveTypeName = parquetField.asPrimitiveType().getPrimitiveTypeName();

            if (logicalTypeAnnotation.equals(stringType())) {
                return BasicTypes.STRING_TYPE;
            }

            if (logicalTypeAnnotation.equals(enumType())) {
                return BasicTypes.STRING_TYPE;
            }

            if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation intType) {
                return switch (intType.getBitWidth()) {
                case 8 -> BasicTypes.BYTE_TYPE;
                case 16 -> BasicTypes.SHORT_TYPE;
                default -> BasicTypes.INT_TYPE;
                };
            }

            if (logicalTypeAnnotation.equals(uuidType())
                    && primitiveTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                return BasicTypes.UUID_TYPE;
            }

            if (logicalTypeAnnotation.equals(dateType()) && primitiveTypeName == PrimitiveTypeName.INT32) {
                return BasicTypes.LOCAL_DATE_TYPE;
            }

            if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time
                    && (primitiveTypeName == PrimitiveTypeName.INT32 || primitiveTypeName == PrimitiveTypeName.INT64)) {
                return BasicTypes.LOCAL_TIME_TYPE;
            }

            if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timeStamp
                    && primitiveTypeName == PrimitiveTypeName.INT64) {
                if (timeStamp.isAdjustedToUTC()) {
                    return BasicTypes.INSTANT_TYPE;
                } else {
                    return BasicTypes.LOCAL_DATE_TIME_TYPE;
                }
            }

            if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalType) {
                return BasicTypes.DECIMAL_TYPE;
            }

            return null;
        }

    }

    private static String extractClassName(String str) {
        String name = str;
        int lastIndexOf = str.lastIndexOf(".");
        if (lastIndexOf > 0) {
            name = str.substring(lastIndexOf + 1);
        }
        return capitalize(name);
    }

    private static String capitalize(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        if (str.length() == 1) {
            return str.toUpperCase();
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

}
