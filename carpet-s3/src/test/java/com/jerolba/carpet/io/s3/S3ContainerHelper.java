package com.jerolba.carpet.io.s3;

import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class S3ContainerHelper {

    static final String BUCKET_NAME = "test-bucket";

    public static LocalStackContainer createS3LocalStackContainer() {
        var localStack = new LocalStackContainer(DockerImageName.parse(
                "localstack/localstack:s3-community-archive:b14111811a1071ff8e05ea2d89fac68dc3aa115bcb0b053f5502a1dfffba4ff8"))
                        .withServices("s3");
        localStack.start();

        System.setProperty("aws.endpointUrl", localStack.getEndpoint().toString());
        System.setProperty("aws.accessKeyId", localStack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localStack.getSecretKey());
        System.setProperty("aws.region", localStack.getRegion());
        return localStack;
    }

    public static S3Client createS3ClientWithBucket(LocalStackContainer localStack) {
        S3Client s3Client = S3Client.builder()
                .endpointOverride(localStack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .build();
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());
        return s3Client;
    }

    public static void stopLocalStack(LocalStackContainer localStack) {
        System.clearProperty("aws.endpointUrl");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");

        if (localStack != null) {
            localStack.stop();
        }
    }

}
