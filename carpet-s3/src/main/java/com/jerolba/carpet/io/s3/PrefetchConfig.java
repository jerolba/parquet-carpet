/**
 * Copyright 2026 Jerónimo López Bezanilla
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
package com.jerolba.carpet.io.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolved configuration of the speculative row group prefetch.
 *
 * @param enabled  whether the next row group is prefetched while the current
 *                 one is being decoded
 * @param maxBytes maximum number of bytes a single speculation may hold
 */
record PrefetchConfig(boolean enabled, long maxBytes) {

    private static final Logger logger = LoggerFactory.getLogger(PrefetchConfig.class);

    /** Default cap of speculative bytes per stream: 256 MiB. */
    static final long DEFAULT_MAX_BYTES = 256L * 1024 * 1024;

    PrefetchConfig {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0, got: " + maxBytes);
        }
    }

    static PrefetchConfig disabled() {
        return new PrefetchConfig(false, DEFAULT_MAX_BYTES);
    }

    /**
     * Resolves the configuration with the precedence: explicit value, system
     * property, default.
     *
     * @param enabled  explicit value from the builder, or null
     * @param maxBytes explicit value from the builder, or null
     * @return the resolved configuration
     */
    static PrefetchConfig resolve(Boolean enabled, Long maxBytes) {
        boolean resolvedEnabled = enabled != null ? enabled
                : booleanProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, true);
        long resolvedMaxBytes = maxBytes != null ? maxBytes
                : longProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, DEFAULT_MAX_BYTES);
        return new PrefetchConfig(resolvedEnabled, resolvedMaxBytes);
    }

    private static boolean booleanProperty(String name, boolean defaultValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        logger.warn("Ignoring invalid boolean value '{}' of system property {}, using {}", value, name, defaultValue);
        return defaultValue;
    }

    private static long longProperty(String name, long defaultValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            // fall through
        }
        logger.warn("Ignoring invalid value '{}' of system property {}, using {}", value, name, defaultValue);
        return defaultValue;
    }

}
