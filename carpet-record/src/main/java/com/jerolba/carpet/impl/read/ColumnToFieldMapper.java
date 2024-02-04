package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.FieldMatchingStrategy.BEST_EFFORT_STRATEGY;
import static com.jerolba.carpet.FieldMatchingStrategy.FIELD_NAME_STRATEGY;
import static com.jerolba.carpet.FieldMatchingStrategy.SNAKE_CASE_STRATEGY;
import static com.jerolba.carpet.impl.CaseConverter.camelToSnakeCase;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.FieldMatchingStrategy;
import com.jerolba.carpet.impl.AliasField;

public class ColumnToFieldMapper {

    public record NameMap(RecordComponent recordComponent, Type parquetType) {
    }

    private final FieldMatchingStrategy fieldMatchingStrategy;

    public ColumnToFieldMapper(FieldMatchingStrategy fieldMatchingStrategy) {
        this.fieldMatchingStrategy = fieldMatchingStrategy;
    }

    public Map<String, NameMap> mapFields(GroupType schema, RecordComponent[] recordComponent) {
        var mapper = new RecordMapper(recordComponent, schema);
        mapper.mapWith(AliasField::getComponentAlias);
        if (fieldMatchingStrategy == FIELD_NAME_STRATEGY || fieldMatchingStrategy == BEST_EFFORT_STRATEGY) {
            mapper.mapWith(RecordComponent::getName);
        }
        if (fieldMatchingStrategy == SNAKE_CASE_STRATEGY || fieldMatchingStrategy == BEST_EFFORT_STRATEGY) {
            mapper.mapWith(component -> camelToSnakeCase(component.getName()));
        }
        return mapper.getResult();
    }

    private class RecordMapper {

        private final Map<String, NameMap> result = new HashMap<>();
        private final RecordComponent[] recordComponents;
        private final Map<String, Type> parquetIndexed;

        public RecordMapper(RecordComponent[] recordComponents, GroupType schema) {
            this.recordComponents = recordComponents;
            this.parquetIndexed = schema.getFields().stream().collect(toMap(Type::getName, f -> f));
        }

        public void mapWith(Function<RecordComponent, String> mapper) {
            for (var component : recordComponents) {
                if (!result.containsKey(component.getName())) {
                    String toTest = mapper.apply(component);
                    Type type = parquetIndexed.get(toTest);
                    if (type != null) {
                        result.put(component.getName(), new NameMap(component, type));
                    }
                }
            }
        }

        public Map<String, NameMap> getResult() {
            return result;
        }

    }

}
