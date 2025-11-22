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
    private final Integer fieldId;

    BinaryType(boolean isNotNull, Integer fieldId) {
        this.isNotNull = isNotNull;
        this.fieldId = fieldId;
    }

    @Override
    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Integer fieldId() {
        return fieldId;
    }

    public BinaryType notNull() {
        return new BinaryType(true, fieldId);
    }

    public BinaryType fieldId(Integer fieldId) {
        return new BinaryType(isNotNull, fieldId);
    }

    public BinaryAliasedType asString() {
        return new BinaryAliasedType(isNotNull, fieldId, BinaryLogicalType.STRING);
    }

    public BinaryAliasedType asEnum() {
        return new BinaryAliasedType(isNotNull, fieldId, BinaryLogicalType.ENUM);
    }

    public BinaryAliasedType asJson() {
        return new BinaryAliasedType(isNotNull, fieldId, BinaryLogicalType.JSON);
    }

    public BinaryAliasedType asBson() {
        return new BinaryAliasedType(isNotNull, fieldId, BinaryLogicalType.BSON);
    }

    public BinaryGeospatialType asParquetGeometry(String crs) {
        return new BinaryGeospatialType(isNotNull, fieldId, GeospatialType.GEOMETRY, crs, null);
    }

    public BinaryGeospatialType asParquetGeography(String crs, EdgeInterpolationAlgorithm algorithm) {
        return new BinaryGeospatialType(isNotNull, fieldId, GeospatialType.GEOGRAPHY, crs, algorithm);
    }

    @Override
    public Class<Binary> getClassType() {
        return Binary.class;
    }

}