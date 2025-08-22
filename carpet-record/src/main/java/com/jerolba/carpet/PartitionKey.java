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
package com.jerolba.carpet;

import java.util.function.Function;

/**
 * Represents a partition key with a name and extraction function.
 * 
 * @param <T> The type of records being partitioned
 * @param name The name of the partition key
 * @param extractor Function to extract the partition value from a record
 */
public record PartitionKey<T>(
    String name, 
    Function<T, String> extractor
) {
    
    public PartitionKey {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Partition key name cannot be null or empty");
        }
        if (extractor == null) {
            throw new IllegalArgumentException("Partition extractor cannot be null");
        }
    }
}
