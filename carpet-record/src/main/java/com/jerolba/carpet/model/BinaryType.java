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
import org.apache.parquet.io.api.Binary;

import com.jerolba.carpet.model.GeometryType.GeospatialType;

public sealed class BinaryType implements FieldType permits BinaryGeospatialType, BinaryAliasedType {

    private final boolean isNotNull;

    BinaryType(boolean isNotNull) {
        this.isNotNull = isNotNull;
    }

    @Override
    public boolean isNotNull() {
        return isNotNull;
    }

    public BinaryType notNull() {
        return new BinaryType(true);
    }

    public BinaryAliasedType asString() {
        return new BinaryAliasedType(isNotNull, BinaryLogicalType.STRING);
    }

    public BinaryAliasedType asEnum() {
        return new BinaryAliasedType(isNotNull, BinaryLogicalType.ENUM);
    }

    public BinaryAliasedType asJson() {
        return new BinaryAliasedType(isNotNull, BinaryLogicalType.JSON);
    }

    public BinaryAliasedType asBson() {
        return new BinaryAliasedType(isNotNull, BinaryLogicalType.BSON);
    }

    public BinaryGeospatialType asParquetGeometry(String crs) {
        return new BinaryGeospatialType(isNotNull, GeospatialType.GEOMETRY, crs, null);
    }

    public BinaryGeospatialType asParquetGeography(String crs, EdgeInterpolationAlgorithm algorithm) {
        return new BinaryGeospatialType(isNotNull, GeospatialType.GEOGRAPHY, crs, algorithm);
    }

    @Override
    public Class<Binary> getClassType() {
        return Binary.class;
    }

}