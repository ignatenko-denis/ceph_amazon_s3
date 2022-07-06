package com.sample;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CephTest {
    private static final String TMP_DIRECTORY_SRC = "ceph_temp_folder_src_";
    private static final String TMP_DIRECTORY_DEST = "ceph_temp_folder_dest_";

    @Test
    void writeAndReadFile() throws IOException {
        S3Config config = getS3Config();

        File tempDirectorySrc = createTempDirectory(TMP_DIRECTORY_SRC);

        // create test source file
        File sourceFile = File.createTempFile("test_ceph_file_", ".txt", tempDirectorySrc);

        // add data to test file
        try (OutputStream outputStream = new FileOutputStream(sourceFile)) {
            outputStream.write("Message".getBytes());
        }

        // 1. write file to Ceph
        assertTrue(Ceph.writeFile(config, sourceFile));

        // 2. read file from Ceph
        File tempDirectoryDest = createTempDirectory(TMP_DIRECTORY_DEST);
        URI fileFromCephUri = Ceph.readLastFile(config, tempDirectoryDest);
        assertNotNull(fileFromCephUri);

        // 3. validate result
        assertEquals(sourceFile.length(), new File(fileFromCephUri).length());

        try (Reader sourceReader = new BufferedReader(new FileReader(sourceFile));
             Reader fileFromCephReader = new BufferedReader(new FileReader(new File(fileFromCephUri)))) {

            assertTrue(IOUtils.contentEquals(sourceReader, fileFromCephReader));
        }
    }

    private S3Config getS3Config() {
        S3Config result = new S3Config();

        result.setAccessKey("TODO");
        result.setSecretKey("TODO");
        result.setEndpoint("https://sample.com/");
        result.setRootBucket("root-folder-name");

        return result;
    }

    private File createTempDirectory(String prefix) {
        try {
            var permissions = PosixFilePermissions.fromString("rwxr--r--");
            var attr = PosixFilePermissions.asFileAttribute(permissions);
            return Files.createTempDirectory(prefix, attr).toFile();
        } catch (IOException e) {
            log.error("cannot create temp directory", e);
        }

        return null;
    }
}