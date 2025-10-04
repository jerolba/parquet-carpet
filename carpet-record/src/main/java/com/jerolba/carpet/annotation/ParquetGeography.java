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

    /**
     * Enumeration of edge interpolation algorithms for geography data.
     * <p>
     * These algorithms determine how edges between geographic points are
     * interpolated when performing spatial calculations and rendering.
     * </p>
     */
    public enum EdgeAlgorithm {
        /**
         * Spherical interpolation algorithm - assumes Earth is a perfect sphere. Fast
         * but less accurate for precise measurements.
         */
        SPHERICAL(EdgeInterpolationAlgorithm.SPHERICAL),

        /**
         * Vincenty's algorithm - high accuracy for geodesic calculations on ellipsoid.
         * More accurate but computationally intensive.
         */
        VINCENTY(EdgeInterpolationAlgorithm.VINCENTY),

        /**
         * Thomas's algorithm - optimized version of Vincenty's formula. Good balance
         * between accuracy and performance.
         */
        THOMAS(EdgeInterpolationAlgorithm.THOMAS),

        /**
         * Andoyer's algorithm - approximation method for short distances. Fast
         * approximation suitable for short geodesic distances.
         */
        ANDOYER(EdgeInterpolationAlgorithm.ANDOYER),

        /**
         * Karney's algorithm - most accurate geodesic calculations. Highest precision
         * but most computationally expensive.
         */
        KARNEY(EdgeInterpolationAlgorithm.KARNEY),

        /**
         * No algorithm specified - uses system default.
         */
        NULL(null);

        private final EdgeInterpolationAlgorithm algorithm;

        private EdgeAlgorithm(EdgeInterpolationAlgorithm algorithm) {
            this.algorithm = algorithm;
        }

        /**
         * Returns the underlying Parquet EdgeInterpolationAlgorithm.
         *
         * @return the corresponding EdgeInterpolationAlgorithm, or null for NULL
         */
        public EdgeInterpolationAlgorithm getAlgorithm() {
            return algorithm;
        }

    }

    /**
     * Specifies the Coordinate Reference System (CRS) for the geography data.
     * <p>
     * Common values include:
     * <ul>
     * <li>"EPSG:4326" - WGS 84 (World Geodetic System 1984)</li>
     * <li>"EPSG:3857" - Web Mercator projection</li>
     * <li>"" (empty) - uses default CRS</li>
     * </ul>
     * </p>
     *
     * @return the CRS identifier string, empty string for default
     */
    String crs() default "";

    /**
     * Specifies the edge interpolation algorithm for geographic calculations.
     * <p>
     * The algorithm determines how edges between geographic points are calculated,
     * affecting accuracy and performance of spatial operations.
     * </p>
     *
     * @return the edge interpolation algorithm to use, NULL for default
     */
    EdgeAlgorithm algorithm() default EdgeAlgorithm.NULL;

}
