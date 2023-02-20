package com.jerolba.carpet.impl.write;

import static com.jerolba.carpet.impl.write.SimpleCollectionItemConsumerFactory.buildSimpleElementConsumer;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.CarpetWriteConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class MapStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    public MapStructureWriter(RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createMapWriter(ParameterizedMap parametized, RecordField recordField) {
        // Key
        BiConsumer<RecordConsumer, Object> elemKeyConsumer = null;
        Class<?> keyType = parametized.getKeyActualType();
        elemKeyConsumer = buildSimpleElementConsumer(keyType, recordConsumer, carpetConfiguration);

        // Value
        BiConsumer<RecordConsumer, Object> elemValueConsumer = null;
        if (parametized.valueIsCollection()) {
            ParameterizedCollection parametizedChild = parametized.getValueTypeAsCollection();
            Consumer<Object> childWriter = createCollectionWriter(parametizedChild, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.valueIsMap()) {
            ParameterizedMap parametizedChild = parametized.getValueTypeAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> valueType = parametized.getValueActualType();
            elemValueConsumer = buildSimpleElementConsumer(valueType, recordConsumer, carpetConfiguration);
        }
        if (elemValueConsumer == null || elemKeyConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in Map");
        }
        if (recordField != null) {
            return new MapRecordFieldWriter(recordField, elemKeyConsumer, elemValueConsumer);
        }
        // We are referenced by other collection
        var innerKeyStructureWriter = elemKeyConsumer;
        var innerValueStructureWriter = elemValueConsumer;
        return value -> {
            if (value != null) {
                recordConsumer.startGroup();
                writeGroupElement(innerKeyStructureWriter, innerValueStructureWriter, value);
                recordConsumer.endGroup();
            }
        };
    }

    private class MapRecordFieldWriter extends FieldWriter {

        private final BiConsumer<RecordConsumer, Object> innerKeyStructureWriter;
        private final BiConsumer<RecordConsumer, Object> innerValueStructureWriter;

        public MapRecordFieldWriter(RecordField recordField, BiConsumer<RecordConsumer, Object> innerStructureWriter,
                BiConsumer<RecordConsumer, Object> innerValueStructureWriter) {
            super(recordField);
            this.innerKeyStructureWriter = innerStructureWriter;
            this.innerValueStructureWriter = innerValueStructureWriter;
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                recordConsumer.startGroup();
                writeGroupElement(innerKeyStructureWriter, innerValueStructureWriter, value);
                recordConsumer.endGroup();
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

    private void writeGroupElement(BiConsumer<RecordConsumer, Object> innerKeyStructureWriter,
            BiConsumer<RecordConsumer, Object> innerValueStructureWriter, Object value) {

        recordConsumer.startField("key_value", 0);
        Map<?, ?> coll = (Map<?, ?>) value;
        for (var v : coll.entrySet()) {
            recordConsumer.startGroup();

            if (v.getKey() != null) {
                recordConsumer.startField("key", 0);
                innerKeyStructureWriter.accept(recordConsumer, v.getKey());
                recordConsumer.endField("key", 0);
            }

            var mapValue = v.getValue();
            if (mapValue != null) {
                recordConsumer.startField("value", 1);
                innerValueStructureWriter.accept(recordConsumer, mapValue);
                recordConsumer.endField("value", 1);
            }
            recordConsumer.endGroup();
        }
        recordConsumer.endField("key_value", 0);
    }

    private Consumer<Object> createCollectionWriter(ParameterizedCollection collectionClass, RecordField field) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> new OneLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        case TWO -> new TwoLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        case THREE -> new ThreeLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        };
    }
}
