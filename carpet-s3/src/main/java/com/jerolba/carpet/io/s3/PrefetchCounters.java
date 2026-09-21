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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe counters of what a {@link PrefetchingRangeReader} speculated,
 * served, fetched and discarded. Updated from the reader thread and from
 * executor threads.
 */
class PrefetchCounters {

    /** Point-in-time values of the counters. */
    record Stats(long speculationsIssued, long rangesServed, long rangesFetched, long bytesServed,
            long bytesDiscarded) {
    }

    private final AtomicLong speculationsIssued = new AtomicLong();
    private final AtomicLong rangesServed = new AtomicLong();
    private final AtomicLong rangesFetched = new AtomicLong();
    private final AtomicLong bytesServed = new AtomicLong();
    private final AtomicLong bytesDiscarded = new AtomicLong();

    /**
     * A speculative read of the next row group has been issued.
     */
    void speculationIssued() {
        speculationsIssued.incrementAndGet();
    }

    /**
     * A range requested by the reader was served from the speculation.
     *
     * @param bytes length of the served range
     */
    void rangeServed(long bytes) {
        rangesServed.incrementAndGet();
        bytesServed.addAndGet(bytes);
    }

    /**
     * Ranges requested by the reader that the speculation did not hold and went
     * to the network.
     *
     * @param ranges number of ranges fetched
     */
    void rangesFetched(int ranges) {
        rangesFetched.addAndGet(ranges);
    }

    /**
     * Prefetched bytes released without being claimed by the reader.
     *
     * @param bytes number of bytes discarded
     */
    void bytesDiscarded(long bytes) {
        bytesDiscarded.addAndGet(bytes);
    }

    Stats snapshot() {
        return new Stats(speculationsIssued.get(), rangesServed.get(), rangesFetched.get(), bytesServed.get(),
                bytesDiscarded.get());
    }

}
