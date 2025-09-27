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
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeographyLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
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
            ParquetReadOptions readOptions = ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
            try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile, readOptions)) {
                MessageType schema = fileReader.getFileMetaData().getSchema();
                SchemaInspector inspector = new SchemaInspector();
                RecordMetadata generated = inspector.inspectGroup(schema, schema.getName()).recordClass();
                Set<RecordMetadata> allClasses = new TreeTraversal().locateClassesInTree(generated);
                CodeGenerator codeGenerator = new CodeGenerator();
                return allClasses.stream().map(codeGenerator::recordToString).toList();
            }
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
                } else if (logicalType instanceof VariantLogicalTypeAnnotation) {
                    return new BasicType(VARIANT_TYPE, field.getRepetition() == Repetition.REQUIRED);
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
            return notNull ? type.primitive() : type.object();
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

    private interface BasicTypes {

        default String primitive() {
            return null;
        }

        String object();

    }

    private record BasicTypeInfo(String primitive, String object) implements BasicTypes {
        public BasicTypeInfo(String object) {
            this(object, object);
        }
    }

    private static class GeometryType implements BasicTypes {

        private final String csr;

        public GeometryType(String csr) {
            this.csr = csr;
        }

        @Override
        public String object() {
            if (csr != null) {
                return "@ParquetGeometry(" + csr + ")";
            }
            return "@ParquetGeometry";
        }

    }

    private static class GeographyType implements BasicTypes {

        private final String csr;
        private final EdgeInterpolationAlgorithm algorithm;

        public GeographyType(String csr, EdgeInterpolationAlgorithm algorithm) {
            this.csr = csr;
            this.algorithm = algorithm;
        }

        @Override
        public String object() {
            String algoParam = algorithm != null ? "algorithm=" + algorithm.name() : "";
            String csrParam = csr != null ? "csr=\"" + csr + "\"" : "";
            if (csrParam.isEmpty() && algoParam.isEmpty()) {
                return "@ParquetGeography";
            }
            String params = new StringJoiner(", ")
                    .add(csrParam)
                    .add("EdgeInterpolationAlgorithm." + algoParam)
                    .toString();
            return "@ParquetGeography(" + params + ")";
        }

    }

    private static class BigDecimalType implements BasicTypes {

        private final int scale;
        private final int precision;

        public BigDecimalType(int scale, int precision) {
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public String object() {
            return "@PrecisionScale(precision = " + precision + ", scale = " + scale + ") BigDecimal";
        }

    }

    private static class BinaryAnnotatedType implements BasicTypes {

        private final String annotation;
        private final String object;

        public BinaryAnnotatedType(String annotation, String object) {
            this.annotation = annotation;
            this.object = object;
        }

        @Override
        public String object() {
            return "@" + annotation + " " + object;
        }

    }

    private static final BasicTypeInfo BYTE_TYPE = new BasicTypeInfo("byte", "Byte");
    private static final BasicTypeInfo SHORT_TYPE = new BasicTypeInfo("short", "Short");
    private static final BasicTypeInfo INT_TYPE = new BasicTypeInfo("int", "Integer");
    private static final BasicTypeInfo LONG_TYPE = new BasicTypeInfo("long", "Long");
    private static final BasicTypeInfo FLOAT_TYPE = new BasicTypeInfo("float", "Float");
    private static final BasicTypeInfo DOUBLE_TYPE = new BasicTypeInfo("double", "Double");
    private static final BasicTypeInfo BOOLEAN_TYPE = new BasicTypeInfo("boolean", "Boolean");
    private static final BasicTypeInfo STRING_TYPE = new BasicTypeInfo("String");
    private static final BasicTypeInfo ENUM_TYPE = new BasicTypeInfo("String");
    private static final BasicTypeInfo UUID_TYPE = new BasicTypeInfo("UUID");
    private static final BasicTypeInfo LOCAL_DATE_TYPE = new BasicTypeInfo("LocalDate");
    private static final BasicTypeInfo LOCAL_TIME_TYPE = new BasicTypeInfo("LocalTime");
    private static final BasicTypeInfo LOCAL_DATE_TIME_TYPE = new BasicTypeInfo("LocalDateTime");
    private static final BasicTypeInfo INSTANT_TYPE = new BasicTypeInfo("Instant");
    private static final BasicTypeInfo BINARY_TYPE = new BasicTypeInfo("Binary");
    private static final BasicTypeInfo VARIANT_TYPE = new BasicTypeInfo("Variant");

    private static class PrimitiveFieldFactory {

        static FieldType buildRecordField(Type parquetField) {
            PrimitiveTypeName typeName = parquetField.asPrimitiveType().getPrimitiveTypeName();
            BasicTypes basicType = buildFromLogicalType(parquetField);
            if (basicType == null) {
                basicType = switch (typeName) {
                case INT32 -> INT_TYPE;
                case INT64 -> LONG_TYPE;
                case FLOAT -> FLOAT_TYPE;
                case DOUBLE -> DOUBLE_TYPE;
                case BOOLEAN -> BOOLEAN_TYPE;
                case BINARY -> BINARY_TYPE;
                default -> throw new RecordTypeConversionException(typeName + " deserialization not supported");
                };
            }

            return new BasicType(basicType, parquetField.isRepetition(Repetition.REQUIRED));
        }

        static BasicTypes buildFromLogicalType(Type parquetField) {
            var logicalType = parquetField.getLogicalTypeAnnotation();
            if (logicalType == null) {
                return null;
            }

            if (logicalType.equals(stringType())) {
                return STRING_TYPE;
            }
            if (logicalType.equals(enumType())) {
                return ENUM_TYPE;
            }
            if (logicalType.equals(jsonType())) {
                return new BinaryAnnotatedType("ParquetJson", "String");
            }
            if (logicalType.equals(bsonType())) {
                return new BinaryAnnotatedType("ParquetBson", "Binary");
            }

            if (logicalType instanceof IntLogicalTypeAnnotation intType) {
                return switch (intType.getBitWidth()) {
                case 8 -> BYTE_TYPE;
                case 16 -> SHORT_TYPE;
                default -> INT_TYPE;
                };
            }

            if (logicalType instanceof DecimalLogicalTypeAnnotation decimal) {
                return new BigDecimalType(decimal.getScale(), decimal.getPrecision());
            }

            var primitiveTypeName = parquetField.asPrimitiveType().getPrimitiveTypeName();
            if (logicalType.equals(uuidType()) && primitiveTypeName == FIXED_LEN_BYTE_ARRAY) {
                return UUID_TYPE;
            }

            if (logicalType.equals(dateType()) && primitiveTypeName == INT32) {
                return LOCAL_DATE_TYPE;
            }

            if (logicalType instanceof TimeLogicalTypeAnnotation
                    && (primitiveTypeName == INT32 || primitiveTypeName == INT64)) {
                return LOCAL_TIME_TYPE;
            }

            if (logicalType instanceof TimestampLogicalTypeAnnotation timeStamp && primitiveTypeName == INT64) {
                if (timeStamp.isAdjustedToUTC()) {
                    return INSTANT_TYPE;
                } else {
                    return LOCAL_DATE_TIME_TYPE;
                }
            }

            if (logicalType instanceof GeometryLogicalTypeAnnotation geometry) {
                String crs = geometry.getCrs();
                return new GeometryType(crs);
            }
            if (logicalType instanceof GeographyLogicalTypeAnnotation geography) {
                String crs = geography.getCrs();
                EdgeInterpolationAlgorithm algorithm = geography.getAlgorithm();
                return new GeographyType(crs, algorithm);
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
        if (str == null || str.isEmpty()) {
            return str;
        }
        if (str.length() == 1) {
            return str.toUpperCase();
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

}
