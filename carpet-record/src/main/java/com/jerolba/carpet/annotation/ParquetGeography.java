package com.jerolba.carpet.annotation;

import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;

@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface ParquetGeography {

    public enum EdgeAlgorithm {
        SPHERICAL(EdgeInterpolationAlgorithm.SPHERICAL),
        VINCENTY(EdgeInterpolationAlgorithm.VINCENTY),
        THOMAS(EdgeInterpolationAlgorithm.THOMAS),
        ANDOYER(EdgeInterpolationAlgorithm.ANDOYER),
        KARNEY(EdgeInterpolationAlgorithm.KARNEY),
        NULL(null);

        private final EdgeInterpolationAlgorithm algorithm;

        private EdgeAlgorithm(EdgeInterpolationAlgorithm value) {
            this.algorithm = value;
        }

        public EdgeInterpolationAlgorithm getAlgorithm() {
            return algorithm;
        }

    }

    String crs() default "";

    EdgeAlgorithm edgeAlgorithm() default EdgeAlgorithm.NULL;

}
