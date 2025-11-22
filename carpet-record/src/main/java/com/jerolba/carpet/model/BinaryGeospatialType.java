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

public final class BinaryGeospatialType extends BinaryType {

    private final GeospatialType geospatialType;
    private final String crs;
    private final EdgeInterpolationAlgorithm algorithm;

    BinaryGeospatialType(boolean isNotNull, Integer fieldId, GeospatialType geospatialType, String crs,
            EdgeInterpolationAlgorithm algorithm) {
        super(isNotNull, fieldId);
        this.geospatialType = geospatialType;
        this.crs = crs;
        this.algorithm = algorithm;
    }

    @Override
    public BinaryGeospatialType notNull() {
        return new BinaryGeospatialType(true, fieldId(), geospatialType, crs, algorithm);
    }

    @Override
    public BinaryGeospatialType fieldId(Integer fieldId) {
        return new BinaryGeospatialType(isNotNull(), fieldId, geospatialType, crs, algorithm);
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

}
