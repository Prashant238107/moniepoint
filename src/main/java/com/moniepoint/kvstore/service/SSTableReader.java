package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.pojo.KeyValue;

import java.io.BufferedReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SSTableReader {

    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    private final Path sstableFile;
    private final Comparator<String> comparator;

    public SSTableReader(Path sstableFile, Comparator<String> comparator) {
        this.sstableFile = sstableFile;
        this.comparator = comparator;
    }

    public List<KeyValue> findInRange(String startKey, String endKey) throws IOException {
        List<KeyValue> results = new ArrayList<>();
        logger.debug("SSTableReader: Reading range [{}, {}] from file: {}", startKey, endKey, sstableFile.getFileName());
        try (BufferedReader reader = Files.newBufferedReader(sstableFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];
                    logger.trace("SSTableReader: Read line for key '{}'", key);
                    if (comparator.compare(key, endKey) > 0) {
                        logger.debug("SSTableReader: Key '{}' is beyond endKey '{}'. Breaking.", key, endKey);
                        break;
                    }
                    if (comparator.compare(key, startKey) >= 0 && comparator.compare(key, endKey) <= 0) {
                        results.add(new KeyValue(key, value));
                    }
                }
            }
        }
        return results;
    }

    public List<KeyValue> readAll() throws IOException {
        List<KeyValue> results = new ArrayList<>();
        logger.debug("SSTableReader: Reading all entries from file: {}", sstableFile.getFileName());
        try (BufferedReader reader = Files.newBufferedReader(sstableFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    logger.trace("SSTableReader: Read line for key '{}'", parts[0]);
                    results.add(new KeyValue(parts[0], parts[1]));
                }
            }
        }
        return results;
    }
}
