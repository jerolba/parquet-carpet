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

import static com.jerolba.carpet.io.s3.PrefetchConfig.DEFAULT_MAX_BYTES;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.io.s3.PrefetchTestSupport.ManualExecutor;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class PrefetchConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED);
        System.clearProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES);
    }

    private static S3Client offlineClient() {
        return S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(() -> AwsBasicCredentials.create("foo", "bar"))
                .endpointOverride(URI.create("http://localhost:8080"))
                .build();
    }

    @Test
    void enabledByDefaultWith256MiB() {
        PrefetchConfig config = new PrefetchConfig(null, null);

        assertTrue(config.enabled());
        assertEquals(256 * 1024 * 1024, config.maxBytes());
    }

    @Test
    void systemPropertiesAreUsedWhenBuilderIsSilent() {
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, "false");
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, "1024");

        PrefetchConfig config = new PrefetchConfig(null, null);

        assertFalse(config.enabled());
        assertEquals(1024, config.maxBytes());
    }

    @Test
    void builderValuesTakePrecedenceOverSystemProperties() {
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, "false");
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, "1024");

        PrefetchConfig config = new PrefetchConfig(true, 4096);

        assertTrue(config.enabled());
        assertEquals(4096, config.maxBytes());
    }

    @Test
    void invalidSystemPropertiesFallBackToDefaults() {
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, "maybe");
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, "-5");

        PrefetchConfig config = new PrefetchConfig(null, null);

        assertTrue(config.enabled());
        assertEquals(DEFAULT_MAX_BYTES, config.maxBytes());

        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, "abc");
        assertEquals(DEFAULT_MAX_BYTES, new PrefetchConfig(null, null).maxBytes());
    }

    @Test
    void propertyValuesAreCaseInsensitiveAndTrimmed() {
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, " FALSE ");
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, " 2048 ");

        PrefetchConfig config = new PrefetchConfig(null, null);

        assertFalse(config.enabled());
        assertEquals(2048, config.maxBytes());
    }

    @Test
    void disabledPrefetchKeepsDefaultCap() {
        PrefetchConfig config = new PrefetchConfig(false, DEFAULT_MAX_BYTES);

        assertFalse(config.enabled());
        assertEquals(DEFAULT_MAX_BYTES, config.maxBytes());
    }

    @Test
    void nonPositiveCapIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new PrefetchConfig(true, 0));
        assertThrows(IllegalArgumentException.class, () -> S3InputFile.builder().prefetchMaxBytes(0));
        assertThrows(IllegalArgumentException.class, () -> S3InputFile.builder().prefetchMaxBytes(-1));
    }

    @Test
    void builtInputFileCarriesTheResolvedConfiguration() {
        S3InputFile withDefaults = S3InputFile.builder("s3://bucket/key").s3Client(offlineClient()).build();
        S3InputFile disabled = S3InputFile.builder("s3://bucket/key").s3Client(offlineClient())
                .prefetch(false).prefetchMaxBytes(123).build();

        assertTrue(((S3InputFileImpl) withDefaults).prefetchConfig().enabled());
        assertFalse(((S3InputFileImpl) disabled).prefetchConfig().enabled());
        assertEquals(123, ((S3InputFileImpl) disabled).prefetchConfig().maxBytes());
    }

    @Test
    void systemPropertyIsResolvedAtBuildTime() {
        System.setProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, "false");
        S3InputFile built = S3InputFile.builder("s3://bucket/key").s3Client(offlineClient()).build();
        System.clearProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED);

        assertFalse(((S3InputFileImpl) built).prefetchConfig().enabled());
    }

    @Nested
    class RangeReaderFactory {

        private final SeekableReader reader = readerOf(sequentialBytes(100));
        private final PrefetchConfig prefetchOn = new PrefetchConfig(true, 1024);

        @Test
        void withoutExecutorReadsSequentiallyEvenIfPrefetchIsEnabled() {
            try (RangeReader created = prefetchOn.create(reader, null)) {
                assertInstanceOf(SequentialReader.class, created);
            }
        }

        @Test
        void withExecutorAndPrefetchDisabledReadsVectored() {
            PrefetchConfig config = new PrefetchConfig(false, DEFAULT_MAX_BYTES);
            try (RangeReader created = config.create(reader, new ManualExecutor())) {
                assertInstanceOf(VectorReader.class, created);
            }
        }

        @Test
        void withExecutorAndPrefetchEnabledDecoratesTheVectoredReader() {
            try (RangeReader created = prefetchOn.create(reader, new ManualExecutor())) {
                assertInstanceOf(PrefetchingRangeReader.class, created);
            }
        }

    }
}
