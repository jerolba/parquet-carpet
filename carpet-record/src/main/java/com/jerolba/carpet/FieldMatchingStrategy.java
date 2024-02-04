package com.jerolba.carpet;

public enum FieldMatchingStrategy {
    FIELD_NAME_STRATEGY, // Field name must match column name
    SNAKE_CASE_STRATEGY, // Field name converted to snake_case must match column name
    BEST_EFFORT_STRATEGY;// Combines previous strategies until match each field with a column
}
