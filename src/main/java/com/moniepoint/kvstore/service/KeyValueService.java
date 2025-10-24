package com.moniepoint.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.entity.KeyValue;
import com.moniepoint.kvstore.entity.WalEntry;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class KeyValueService {

    @Value("${kvstore.wal.file}")
    private String walFilePath;

    @Value("${kvstore.sstable.path}")
    private String sstablePath;

    @Value("${kvstore.memtable.max-size-bytes}")
    private long memtableMaxSize;

    private final ConcurrentNavigableMap<String, String> memtable = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BufferedWriter walWriter;
    private long memtableSizeInBytes = 0;
    private List<Path> sstables = new ArrayList<>();

    private static final String DELETED_MARKER = "__DELETED__";

    @PostConstruct
    public void init() throws IOException {
        Path sstableDir = Paths.get(sstablePath);
        if (!Files.exists(sstableDir)) {
            Files.createDirectories(sstableDir);
        }

        walWriter = new BufferedWriter(new FileWriter(walFilePath, true));
        recoverState();
        loadSSTables(); // Load existing SSTables on startup
    }

    @PreDestroy
    public void shutdown() throws IOException {
        flushMemtableToSSTable();
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
                        memtable.put(entry.getKey(), DELETED_MARKER); // Use marker for tombstone
                    } else {
                        memtable.put(entry.getKey(), entry.getValue());
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
        memtable.put(key, value);
        memtableSizeInBytes += key.length() + value.length(); // Approximate size
        if (memtableSizeInBytes > memtableMaxSize) {
            flushMemtableToSSTable();
        }
    }

    public void batchPut(List<KeyValue> keyValues) throws IOException {
        for (KeyValue kv : keyValues) {
            put(kv.getKey(), kv.getValue());
        }
    }

    public Optional<String> read(String key) throws IOException {
        // 1. Check MemTable first
        String value = memtable.get(key);
        if (value != null) {
            if (value.equals(DELETED_MARKER)) {
                return Optional.empty(); // Found tombstone in memtable
            }
            return Optional.of(value);
        }

        // 2. Check SSTables from newest to oldest
        for (Path sstableFile : sstables) {
            try (BufferedReader reader = new BufferedReader(new FileReader(sstableFile.toFile()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && parts[0].equals(key)) {
                        if (parts[1].equals(DELETED_MARKER)) {
                            return Optional.empty(); // Found tombstone in SSTable
                        } else {
                            return Optional.of(parts[1]); // Found value in SSTable
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    public List<KeyValue> readKeyRange(String startKey, String endKey) throws IOException {
        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
        memtable.subMap(startKey, true, endKey, true)
                .forEach(mergedResults::put);
        for (Path sstableFile : sstables) {
            SSTableReader reader = new SSTableReader(sstableFile, new NaturalKeyComparator());
            List<KeyValue> sstableKVs = reader.findInRange(startKey, endKey);
            for (KeyValue kv : sstableKVs) {
                mergedResults.putIfAbsent(kv.getKey(), kv.getValue());
            }
        }
        return mergedResults.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(DELETED_MARKER))
                .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public void delete(String key) throws IOException {
        WalEntry entry = new WalEntry(key, null, true);
        walWriter.write(objectMapper.writeValueAsString(entry) + "\n");
        walWriter.flush();
        memtable.put(key, DELETED_MARKER); // Use marker for tombstone
    }

    private void flushMemtableToSSTable() throws IOException {
        if (memtable.isEmpty()) {
            return;
        }

        long timestamp = System.currentTimeMillis();
        Path sstableFile = Paths.get(sstablePath, "sstable-" + timestamp + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sstableFile.toFile()))) {
            for (var entry : memtable.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
        }

        sstables.add(0, sstableFile);
        memtable.clear();
        memtableSizeInBytes = 0;
    }

    private void loadSSTables() throws IOException {
        try (Stream<Path> paths = Files.list(Paths.get(sstablePath))) {
            sstables = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("sstable-"))
                    .sorted(Collections.reverseOrder()) // Newest first
                    .collect(Collectors.toList());
        }
    }
}
