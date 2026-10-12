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

import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * LocalStack for the tests that need a real S3 API.
 * <p>
 * One container is shared by every test class of the JVM, since starting one
 * takes several seconds: a class attaches the default S3 client to it with
 * {@link #startSharedLocalStack()} and detaches it with
 * {@link #clearLocalStackProperties()}. Objects written by one class stay
 * visible to the next, so a key must not be reused across classes with
 * different expectations.
 * </p>
 */
class S3ContainerHelper {

    static final String BUCKET_NAME = "test-bucket";

    private static LocalStackContainer sharedContainer;

    /**
     * Starts the shared container on first use and configures the default S3
     * client, through the endpoint, credentials and region system properties, to
     * use it. The container stops when the JVM exits.
     */
    static synchronized LocalStackContainer startSharedLocalStack() {
        if (sharedContainer == null) {
            var container = new LocalStackContainer(DockerImageName.parse(
                    "localstack/localstack:s3-community-archive:b14111811a1071ff8e05ea2d89fac68dc3aa115bcb0b053f5502a1dfffba4ff8"))
                            .withServices("s3");
            container.start();
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop, "localstack-stop"));
            sharedContainer = container;
        }
        System.setProperty("aws.endpointUrl", sharedContainer.getEndpoint().toString());
        System.setProperty("aws.accessKeyId", sharedContainer.getAccessKey());
        System.setProperty("aws.secretAccessKey", sharedContainer.getSecretKey());
        System.setProperty("aws.region", sharedContainer.getRegion());
        return sharedContainer;
    }

    /**
     * A client for the container, with {@link #BUCKET_NAME} created if a previous
     * test class did not create it already.
     */
    public static S3Client createS3ClientWithBucket(LocalStackContainer localStack) {
        S3Client s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .build();
        try {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(BUCKET_NAME)
                    .build());
        } catch (BucketAlreadyOwnedByYouException e) {
            // Created by a previous test class
        }
        return s3Client;
    }

    /**
     * Detaches the default S3 client from the container, which keeps running for
     * the next test class.
     */
    static void clearLocalStackProperties() {
        System.clearProperty("aws.endpointUrl");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

}
