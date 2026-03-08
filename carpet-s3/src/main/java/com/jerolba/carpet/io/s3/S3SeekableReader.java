package com.jerolba.carpet.io.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

class S3SeekableReader implements SeekableReader {

    private static final Logger logger = LoggerFactory.getLogger(S3SeekableReader.class);

    private final S3Client client;
    private final String bucket;
    private final String key;
    private Long length;
    private String etag;

    public S3SeekableReader(S3Client client, String bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public long getLength() throws IOException {
        if (length == null) {
            try {
                HeadObjectRequest headRequest = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                HeadObjectResponse headResponse = client.headObject(headRequest);
                length = headResponse.contentLength();
                etag = headResponse.eTag().replaceAll("\"", ""); // Remove quotes from ETag if present
            } catch (Exception e) {
                throw new IOException("Failed to get length of S3 object: " + bucket + "/" + key, e);
            }
        }
        return length;
    }

    @Override
    public int readFully(long pos, byte[] b, int off, int len) throws IOException {
        try {
            GetObjectRequest getObjectRequest = createGetRequest(pos, len);

            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            if (data.length < len) {
                throw new IOException("Expected " + len + " bytes but got " + data.length + " bytes from S3");
            }
            System.arraycopy(data, 0, b, off, len);
            return len;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to read from S3 object: " + bucket + "/" + key +
                    " at position " + pos + " for " + len + " bytes", e);
        }
    }

    @Override
    public int readFully(long pos, ByteBuffer byteBuffer) throws IOException {
        int len = byteBuffer.remaining();
        try {
            GetObjectRequest getObjectRequest = createGetRequest(pos, len);

            ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            if (data.length < len) {
                throw new IOException("Expected " + len + " bytes but got " + data.length + " bytes from S3");
            }
            byteBuffer.put(data);
            return len;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to read from S3 object: " + bucket + "/" + key +
                    " at position " + pos + " for " + len + " bytes", e);
        }
    }

    private GetObjectRequest createGetRequest(long pos, int len) {
        String range = String.format("bytes=%d-%d", pos, pos + len - 1);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range(range)
                .build();
        return getObjectRequest;
    }

    @Override
    public void close() throws IOException {
        // No resources to close for S3Client-based reads
        // The S3Client lifecycle is managed externally
    }

    /**
     * Downloads the S3 object to a local file in the system's temporary directory.
     * The file name is constructed using the S3 object key file name and ETag to
     * ensure uniqueness and avoid conflicts.
     *
     * @return the Path to the downloaded local file
     * @throws IOException if an error occurs while downloading the file
     */
    public Path downloadToLocalFile() throws IOException {
        getLength(); // Get etag and length
        String tmpDir = System.getProperty("java.io.tmpdir");
        String targetFileName = buildTargetFileName(key, etag);
        Path targetPath = Path.of(tmpDir, targetFileName);
        if (targetPath.toFile().exists() && Files.size(targetPath) == getLength()) {
            logger.info("Using cached local file for S3 object: {}", targetPath);
            return targetPath;
        }
        logger.info("Downloading S3 object to local file: s3://{}/{} -> {}", bucket, key, targetPath);
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            client.getObject(getObjectRequest, targetPath);
            return targetPath;
        } catch (Exception e) {
            throw new IOException("Failed to download S3 object: " + bucket + "/" + key, e);
        }
    }

    private String buildTargetFileName(String key, String etag) {
        int lastIndexOfSlash = key.lastIndexOf('/');
        String fileName = lastIndexOfSlash >= 0 ? key.substring(lastIndexOfSlash + 1) : key;
        int lastIndexOf = fileName.lastIndexOf('.');
        String prefix = lastIndexOf >= 0 ? fileName.substring(0, lastIndexOf) : fileName;
        String suffix = lastIndexOf >= 0 ? fileName.substring(lastIndexOf) : "";
        return prefix + "_" + etag + suffix;
    }

}