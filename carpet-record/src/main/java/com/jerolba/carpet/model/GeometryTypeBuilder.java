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
package com.jerolba.carpet.model;

import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;

import com.jerolba.carpet.model.GeometryType.GeospatialType;

public class GeometryTypeBuilder {

    private final boolean notNull;
    private final Integer fieldId;

    GeometryTypeBuilder() {
        this(false, null);
    }

    private GeometryTypeBuilder(boolean notNull, Integer fieldId) {
        this.notNull = notNull;
        this.fieldId = fieldId;
    }

    public GeometryTypeBuilder notNull() {
        return new GeometryTypeBuilder(true, fieldId);
    }

    public GeometryTypeBuilder fieldId(Integer fieldId) {
        return new GeometryTypeBuilder(notNull, fieldId);
    }

    public GeometryType asParquetGeometry(String crs) {
        return new GeometryType(notNull, fieldId, GeospatialType.GEOMETRY, crs, null);
    }

    public GeometryType asParquetGeography(String crs, EdgeInterpolationAlgorithm algorithm) {
        return new GeometryType(notNull, fieldId, GeospatialType.GEOGRAPHY, crs, algorithm);
    }

}
