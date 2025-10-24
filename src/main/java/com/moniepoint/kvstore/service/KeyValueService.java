package com.moniepoint.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.entity.KeyValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

@Service
public class KeyValueService {

    @Value("${kvstore.wal.file}")
    private String walFilePath;

    private final ConcurrentNavigableMap<String, String> map = new ConcurrentSkipListMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BufferedWriter walWriter;

    @PostConstruct
    public void init() throws IOException {
        walWriter = new BufferedWriter(new FileWriter(walFilePath, true));
        recoverState();
    }

    @PreDestroy
    public void shutdown() throws IOException {
        if (walWriter != null) {
            walWriter.close();
        }
    }

    private void recoverState() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(walFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    WalEntry entry = objectMapper.readValue(line, WalEntry.class);
                    if (entry.isDelete()) {
                        map.remove(entry.getKey());
                    } else {
                        map.put(entry.getKey(), entry.getValue());
                    }
                } catch (IOException e) {
                    System.err.println("Skipping corrupted WAL entry: " + line);
                }
            }
        }
    }

    public void put(String key, String value) throws IOException {
        WalEntry entry = new WalEntry(key, value);
        walWriter.write(objectMapper.writeValueAsString(entry) + "\n");
        walWriter.flush();
        map.put(key, value);
    }

    public void batchPut(List<KeyValue> keyValues) throws IOException {
        for (KeyValue kv : keyValues) {
            put(kv.getKey(), kv.getValue());
        }
    }

    public Optional<String> read(String key) {
        return Optional.ofNullable(map.get(key));
    }

    public List<KeyValue> readKeyRange(String startKey, String endKey) {
        return map.subMap(startKey, true, endKey, true).entrySet().stream()
                .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public void delete(String key) throws IOException {
        WalEntry entry = new WalEntry(key, null, true);
        walWriter.write(objectMapper.writeValueAsString(entry) + "\n");
        walWriter.flush();
        map.remove(key);
    }

    private static class WalEntry {
        private String key;
        private String value;
        private boolean delete;

        public WalEntry() {
        }

        public WalEntry(String key, String value) {
            this(key, value, false);
        }

        public WalEntry(String key, String value, boolean delete) {
            this.key = key;
            this.value = value;
            this.delete = delete;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public boolean isDelete() {
            return delete;
        }
    }
}
