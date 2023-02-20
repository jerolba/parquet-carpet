package com.jerolba.carpet.impl.write;

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.CarpetWriteConfiguration;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    private final List<Consumer<Object>> fieldWriters = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;

        // Preconditions: All fields are writable
        int idx = 0;
        for (RecordComponent attr : recordClass.getRecordComponents()) {
            String fieldName = getFieldName(attr);

            Class<?> type = attr.getType();
            String typeName = type.getName();
            Consumer<Object> writer = null;
            RecordField f = new RecordField(recordClass, attr, fieldName, idx);

            writer = buildBasicTypeWriter(typeName, type, f);

            if (writer == null) {
                if (type.isRecord()) {
                    var recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
                    writer = new RecordFieldWriter(f, recordWriter);
                } else if (Collection.class.isAssignableFrom(type)) {
                    ParameterizedCollection collectionClass = getParameterizedCollection(attr);
                    writer = createCollectionWriter(collectionClass, f);
                } else if (Map.class.isAssignableFrom(type)) {
                    ParameterizedMap mapClass = getParameterizedMap(attr);
                    writer = createMapWriter(mapClass, f);
                } else {
                    System.out.println(typeName + " can not be serialized");
                    // throw new RuntimeException(typeName + " can not be serialized");
                }
            }
            fieldWriters.add(writer);
            idx++;
        }
    }

    private Consumer<Object> createCollectionWriter(ParameterizedCollection collectionClass, RecordField f) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> new OneLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        case TWO -> new TwoLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        case THREE -> new ThreeLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        };
    }

    private Consumer<Object> createMapWriter(ParameterizedMap mapClass, RecordField f) {
        return new MapStructureWriter(recordConsumer, carpetConfiguration).createMapWriter(mapClass, f);

    }

    private FieldWriter buildBasicTypeWriter(String typeName, Class<?> type, RecordField f) {
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new IntegerFieldWriter(f);
        } else if (typeName.equals("java.lang.String")) {
            return new StringFieldWriter(f);
        } else if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanFieldWriter(f);
        } else if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new LongFieldWriter(f);
        } else if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new DoubleFieldWriter(f);
        } else if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FloatFieldWriter(f);
        } else if (typeName.equals("short") || typeName.equals("java.lang.Short") ||
                typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new IntegerCompatibleFieldWriter(f);
        } else if (type.isEnum()) {
            return new EnumFieldWriter(f, type);
        }
        return null;
    }

    public void write(Object record) {
        for (var fieldWriter : fieldWriters) {
            fieldWriter.accept(record);
        }
    }

    private class IntegerFieldWriter extends FieldWriter {

        public IntegerFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addInteger((Integer) value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class IntegerCompatibleFieldWriter extends FieldWriter {

        public IntegerCompatibleFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addInteger(((Number) value).intValue());
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class LongFieldWriter extends FieldWriter {

        public LongFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addLong((Long) value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class BooleanFieldWriter extends FieldWriter {

        public BooleanFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addBoolean((Boolean) value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class FloatFieldWriter extends FieldWriter {

        public FloatFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addFloat((Float) value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class DoubleFieldWriter extends FieldWriter {

        public DoubleFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addDouble((Double) value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class StringFieldWriter extends FieldWriter {

        public StringFieldWriter(RecordField recordField) {
            super(recordField);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addBinary(Binary.fromString((String) value));
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private class EnumFieldWriter extends FieldWriter {

        private final EnumsValues values;

        public EnumFieldWriter(RecordField recordField, Class<?> enumClass) {
            super(recordField);
            values = new EnumsValues(enumClass);
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.addBinary(values.getValue(value));
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }

    }

    private class RecordFieldWriter extends FieldWriter {

        private final CarpetRecordWriter writer;

        public RecordFieldWriter(RecordField recordField, CarpetRecordWriter writer) {
            super(recordField);
            this.writer = writer;
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.startGroup();
                writer.write(value);
                recordConsumer.endGroup();
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

}
