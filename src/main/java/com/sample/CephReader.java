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
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Slf4j
public class CephReader {
    private static final String TMP_DIRECTORY_PREFIX = "ceph_temp_folder_";
    private static final ZoneId ZONE = ZoneId.systemDefault();
    private static final LocalDateTime DEFAULT_DATE_TIME = LocalDateTime.of(1, 1, 1, 1, 1, 1);

    public URI readLastFile(S3Config config) {
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

        // creating temporary local folder
        var tmpDir = createTempDirectory();
        if (tmpDir == null) {
            log.error("Cannot create temporary local directory");
            return null;
        }

        // creating temporary local file
        String tmpFileName = buildTempFileName(tmpDir, objectSummary.getKey());
        var tmpFile = new File(tmpFileName);
        log.info("created temporary file: {}", tmpFile.toURI());

        log.info("start downloading File from Ceph...");
        ObjectMetadata object = s3.getObject(
                new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey()), tmpFile);
        log.info("File length: {} bytes", object.getContentLength());
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

    private AmazonS3 connect(S3Config s3Config) {
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

    private Bucket foundBucketByName(List<Bucket> buckets, String bucketName) {
        for (Bucket bucket : buckets) {
            if (bucket.getName().equals(bucketName)) {
                return bucket;
            }
        }

        return null;
    }

    private void printAllFiles(ObjectListing listing) {
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        log.info("found '{}' files", objectSummaries.size());

        for (S3ObjectSummary objectSummary : objectSummaries) {
            log.info("{}, {} bytes, {}", objectSummary.getKey(), objectSummary.getSize(),
                    objectSummary.getLastModified());
        }
    }

    private S3ObjectSummary foundLastObjectSummary(ObjectListing listing) {
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

    private File createTempDirectory() {
        try {
            var permissions = PosixFilePermissions.fromString("rwxr--r--");
            var attr = PosixFilePermissions.asFileAttribute(permissions);
            return Files.createTempDirectory(TMP_DIRECTORY_PREFIX, attr).toFile();
        } catch (IOException e) {
            log.error("cannot create temp file", e);
        }

        return null;
    }

    private String buildTempFileName(File tmpDir, String fileName) {
        return tmpDir.getPath() + File.separator + fileName;
    }

    private static LocalDateTime toLocalDateTime(Instant date) {
        return LocalDateTime.ofInstant(date, ZONE);
    }
}
