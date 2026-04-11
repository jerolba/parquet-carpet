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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.parquet.io.PositionOutputStream;

class LocalOutputFile implements S3OutputFile {

    private final Path path;

    LocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("Path already exists: " + path);
        }
        return createOrOverwrite(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new CountedPositionOutputStream(Files.newOutputStream(path));
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return path.toString();
    }

    private static class CountedPositionOutputStream extends PositionOutputStream {

        private final BufferedOutputStream bos;
        private long pos = 0;

        CountedPositionOutputStream(OutputStream os) {
            this.bos = new BufferedOutputStream(os);
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void flush() throws IOException {
            bos.flush();
        }

        @Override
        public void close() throws IOException {
            bos.close();
        }

        @Override
        public void write(int b) throws IOException {
            bos.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            bos.write(b, off, len);
            pos += len;
        }

        @Override
        public void write(byte[] b) throws IOException {
            bos.write(b);
            pos += b.length;
        }
    }

}
