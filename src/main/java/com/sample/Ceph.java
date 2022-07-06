package com.sample;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static lombok.AccessLevel.PRIVATE;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class Ceph {
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDateTime DEFAULT_DATE_TIME = LocalDateTime.of(1, 1, 1, 1, 1, 1);

    public static boolean writeFile(S3Config config, File file) {
        // connection to the Ceph
        AmazonS3 s3 = connect(config);
        if (s3 == null) {
            log.error("cannot connect to Ceph");
            return false;
        }

        List<Bucket> buckets;

        try {
            buckets = s3.listBuckets();
        } catch (Exception e) {
            log.error("cannot connect to Ceph", e);
            return false;
        }

        log.info("Ceph connection initiated!");

        var bucket = foundBucketByName(buckets, config.getRootBucket());
        if (bucket == null) {
            log.error("Cannot read Bucket from Ceph");
            return false;
        }

        log.info("start uploading file '{}' to Ceph...", file.getName());
        try {
            PutObjectRequest request = new PutObjectRequest(bucket.getName(), file.getName(), file);
            s3.putObject(request);
            log.info("uploading '{}' finished!", file.getName());
        } catch (Exception e) {
            log.error(String.format("Cannot upload file '%s' to Ceph", file.getName()), e);
            return false;
        }

        return true;
    }

    public static URI readLastFile(S3Config config, File destinationDirectory) {
        // connection to the Ceph
        AmazonS3 s3 = connect(config);
        if (s3 == null) {
            log.error("cannot connect to Ceph");
            return null;
        }

        List<Bucket> buckets;

        try {
            buckets = s3.listBuckets();
        } catch (Exception e) {
            log.error("cannot connect to Ceph", e);
            return null;
        }

        log.info("Ceph connection initiated!");

        var bucket = foundBucketByName(buckets, config.getRootBucket());
        if (bucket == null) {
            log.error("Cannot read Bucket from Ceph");
            return null;
        }

        ObjectListing listing = s3.listObjects(bucket.getName());
        printAllFiles(listing);

        S3ObjectSummary objectSummary = foundLastObjectSummary(listing);
        if (objectSummary == null) {
            log.error("Cannot read last actual S3ObjectSummary from Ceph");
            return null;
        }
        log.info("founded last modified file: {}", objectSummary.getKey());

        // creating temporary local file
        String tmpFileName = buildTempFileName(destinationDirectory, objectSummary.getKey());
        var tmpFile = new File(tmpFileName);
        log.info("created temporary file: {}", tmpFile.toURI());

        log.info("start downloading File from Ceph...");
        GetObjectRequest request = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey());
        ObjectMetadata object = s3.getObject(request, tmpFile);
        log.info("Ceph file length: {} bytes", object.getContentLength());
        log.info("downloaded local file length: {} bytes", tmpFile.length());

        if (object.getContentLength() != tmpFile.length()) {
            log.error("File length is different. File is corrupted!");
            return null;
        }

        log.info("downloaded local file content:");
        try (BufferedReader br = new BufferedReader(new FileReader(tmpFile))) {
            br.lines().forEach(log::info);
        } catch (Exception e) {
            log.error("cannot print downloaded file", e);
        }

        return tmpFile.toURI();
    }

    private static AmazonS3 connect(S3Config s3Config) {
        // Disable checking certification
        try {
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setProtocol(Protocol.HTTP);
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
            if (SDKGlobalConfiguration.isCertCheckingDisabled()) {
                log.info("Certification checking is disabled");
            }
        } catch (Exception e) {
            log.error("cannot disable certification", e);
            return null;
        }

        AWSCredentials credentials = new BasicAWSCredentials(
                s3Config.getAccessKey(), s3Config.getSecretKey());
        var credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        var endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(
                        s3Config.getEndpoint(),
                        Regions.DEFAULT_REGION.getName());

        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(endpointConfiguration)
                .withPathStyleAccessEnabled(true)
                .withPayloadSigningEnabled(true)
                .withForceGlobalBucketAccessEnabled(true)
                .build();
    }

    private static Bucket foundBucketByName(List<Bucket> buckets, String bucketName) {
        for (Bucket bucket : buckets) {
            if (bucket.getName().equals(bucketName)) {
                return bucket;
            }
        }

        return null;
    }

    private static void printAllFiles(ObjectListing listing) {
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        log.info("found '{}' files", objectSummaries.size());

        for (S3ObjectSummary objectSummary : objectSummaries) {
            log.info("{}, {} bytes, {}", objectSummary.getKey(), objectSummary.getSize(),
                    objectSummary.getLastModified());
        }
    }

    private static S3ObjectSummary foundLastObjectSummary(ObjectListing listing) {
        var last = DEFAULT_DATE_TIME;
        S3ObjectSummary result = null;

        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            var current = toLocalDateTime(summary.getLastModified().toInstant());

            if (current.isAfter(last)) {
                last = current;
                result = summary;
            }
        }

        return result;
    }

    private static String buildTempFileName(File tmpDir, String fileName) {
        return tmpDir.getPath() + File.separator + fileName;
    }

    private static LocalDateTime toLocalDateTime(Instant date) {
        return LocalDateTime.ofInstant(date, ZONE);
    }
}
