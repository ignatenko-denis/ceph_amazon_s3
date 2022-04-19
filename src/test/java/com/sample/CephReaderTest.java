package com.sample;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CephReaderTest {
    private final CephReader cephReader = new CephReader();

    @Test
    void readLastFile() {
        S3Config config = new S3Config();

        config.setAccessKey("KEY");
        config.setSecretKey("KEY");
        config.setEndpoint("https://endpoint.com/");
        config.setRootBucket("root-bucket");

        URI uri = cephReader.readLastFile(config);
        assertNotNull(uri);
    }
}