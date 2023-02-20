package com.jerolba.carpet.impl.write;

import static com.jerolba.carpet.impl.write.SimpleCollectionItemConsumerFactory.buildSimpleElementConsumer;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.CarpetWriteConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class TwoLevelStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    public TwoLevelStructureWriter(RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createCollectionWriter(ParameterizedCollection parametized, RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isCollection()) {
            ParameterizedCollection parametizedChild = parametized.getParametizedAsCollection();
            TwoLevelStructureWriter child = new TwoLevelStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = child.createCollectionWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        if (recordField != null) {
            return new TwoLevelCollectionRecordFieldWriter(recordField, elemConsumer);
        }
        // We are referenced by other collection
        var innerStructureWriter = elemConsumer;
        return value -> {
            if (value != null) {
                writeGroupElement(innerStructureWriter, value);
            }
        };
    }

    private class TwoLevelCollectionRecordFieldWriter extends FieldWriter {

        private final BiConsumer<RecordConsumer, Object> innerStructureWriter;

        public TwoLevelCollectionRecordFieldWriter(RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            super(recordField);
            this.innerStructureWriter = innerStructureWriter;
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                writeGroupElement(innerStructureWriter, value);
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }

    }

    private void writeGroupElement(BiConsumer<RecordConsumer, Object> innerStructureWriter, Object value) {
        recordConsumer.startGroup();
        recordConsumer.startField("element", 0);
        Collection<?> coll = (Collection<?>) value;
        for (var v : coll) {
            if (v == null) {
                throw new NullPointerException("2-level list structures doesn't support null values");
            }
            innerStructureWriter.accept(recordConsumer, v);
        }
        recordConsumer.endField("element", 0);
        recordConsumer.endGroup();
    }

}
