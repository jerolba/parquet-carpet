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
package com.jerolba.carpet.impl.write;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.parquet.io.api.Binary;

class UuidWrite {

    public static Binary uuidToBinary(Object value) {
        UUID uuid = (UUID) value;
        byte[] arr = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(arr);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Binary.fromConstantByteArray(arr);
    }

}
