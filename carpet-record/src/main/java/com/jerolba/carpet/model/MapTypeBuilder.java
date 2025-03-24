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

public class MapTypeBuilder {

    private final boolean notNull;

    MapTypeBuilder() {
        this(false);
    }

    private MapTypeBuilder(boolean notNull) {
        this.notNull = notNull;
    }

    public MapTypeBuilder notNull() {
        return new MapTypeBuilder(true);
    }

    public MapType ofTypes(FieldType keyType, FieldType valueType) {
        return new MapType(notNull, keyType, valueType);
    }

}