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
import org.locationtech.jts.geom.Geometry;

public final class GeometryType implements FieldType {

    public enum GeospatialType {
        GEOMETRY, GEOGRAPHY
    }

    private final boolean isNotNull;
    private final GeospatialType geospatialType;
    private final String crs;
    private final EdgeInterpolationAlgorithm algorithm;

    GeometryType(boolean isNotNull, GeospatialType geospatialType, String crs, EdgeInterpolationAlgorithm algorithm) {
        this.isNotNull = isNotNull;
        this.geospatialType = geospatialType;
        this.crs = crs;
        this.algorithm = algorithm;
    }

    public GeometryType notNull() {
        return new GeometryType(true, geospatialType, crs, algorithm);
    }

    @Override
    public boolean isNotNull() {
        return isNotNull;
    }

    public GeospatialType geospatialType() {
        return geospatialType;
    }

    public String crs() {
        return crs;
    }

    public EdgeInterpolationAlgorithm algorithm() {
        return algorithm;
    }

    @Override
    public Class<?> getClassType() {
        return Geometry.class;
    }

}
