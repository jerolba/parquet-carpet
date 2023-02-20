package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.read.PrimitiveGenericConverterFactory.buildPrimitiveGenericConverters;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;

class CarpetListIntermediateConverter extends GroupConverter {

    private final Converter converter;
    private final ListHolder listHolder;
    private Object elementValue;

    public CarpetListIntermediateConverter(Type rootListType, ParameterizedCollection parameterized,
            ListHolder listHolder) {
        var requestedSchema = rootListType.asGroupType();
        System.out.println(requestedSchema);
        this.listHolder = listHolder;

        List<Type> fields = requestedSchema.getFields();
        if (fields.size() > 1) {
            throw new RecordTypeConversionException(
                    requestedSchema.getName() + " LIST child element can not have more than one field");
        }
        Consumer<Object> consumer = this::accept;
        Type listElement = fields.get(0);
        converter = createCollectionConverter(listElement, parameterized, consumer);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converter;
    }

    @Override
    public void start() {
        elementValue = null;
    }

    @Override
    public void end() {
        listHolder.add(elementValue);
    }

    public void accept(Object value) {
        elementValue = value;
    }

    public static Converter createCollectionConverter(Type listElement, ParameterizedCollection parameterized,
            Consumer<Object> consumer) {
        if (listElement.isPrimitive()) {
            return buildPrimitiveGenericConverters(listElement, parameterized.getActualType(), consumer);
        }
        LogicalTypeAnnotation logicalType = listElement.getLogicalTypeAnnotation();
        if (logicalType == listType() && parameterized.isCollection()) {
            var parameterizedList = parameterized.getParametizedAsCollection();
            return new CarpetListConverter(listElement.asGroupType(), parameterizedList, consumer);
        }
        if (logicalType == mapType() && parameterized.isMap()) {
            var parameterizedMap = parameterized.getParametizedAsMap();
            return new CarpetMapConverter(listElement.asGroupType(), parameterizedMap, consumer);

        }
        GroupType groupType = listElement.asGroupType();
        Class<?> listType = parameterized.getActualType();
        return new CarpetGroupConverter(groupType, listType, consumer);
    }

}