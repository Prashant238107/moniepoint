package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.entity.KeyValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SSTableReader {

    private final Path sstableFile;
    private final Comparator<String> comparator;

    public SSTableReader(Path sstableFile, Comparator<String> comparator) {
        this.sstableFile = sstableFile;
        this.comparator = comparator;
    }

    public List<KeyValue> findInRange(String startKey, String endKey) throws IOException {
        List<KeyValue> results = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(sstableFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];
                    if (comparator.compare(key, endKey) > 0) {
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
        try (BufferedReader reader = Files.newBufferedReader(sstableFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    results.add(new KeyValue(parts[0], parts[1]));
                }
            }
        }
        return results;
    }
}
