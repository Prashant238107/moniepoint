package com.moniepoint.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.entity.KeyValue;
import com.moniepoint.kvstore.entity.SSTableMeta;
import com.moniepoint.kvstore.entity.WalEntry;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.moniepoint.kvstore.constants.Constants.DELETED_MARKER;

@Service
public class KeyValueService {

    @Value("${kvstore.wal.file}")
    private String walFilePath;

    @Value("${kvstore.sstable.path}")
    private String sstablePath;

    @Value("${kvstore.memtable.max-size-bytes}")
    private long memtableMaxSize;

    @Autowired
    private MemTable memTable;

    @Autowired
    private ObjectMapper objectMapper;
    private BufferedWriter walWriter;
    private List<SSTableMeta> sstables = new CopyOnWriteArrayList<>();
    private final NaturalKeyComparator comparator = new NaturalKeyComparator();

    @PostConstruct
    public void init() throws IOException {
        Path sstableDir = Paths.get(sstablePath);
        if (!Files.exists(sstableDir)) {
            Files.createDirectories(sstableDir);
        }
        walWriter = Files.newBufferedWriter(Paths.get(walFilePath), StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        loadSSTables();
        recoverState();
    }

    @PreDestroy
    public void shutdown() throws IOException {
        flushMemtableToSSTable();
        if (walWriter != null) {
            walWriter.close();
        }
    }

    private void recoverState() throws IOException {
        Path walPath = Paths.get(walFilePath);
        if (!Files.exists(walPath)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(walPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    WalEntry entry = objectMapper.readValue(line, WalEntry.class);
                    if (entry.isDeleted()) {
                        memTable.put(entry.getKey(), DELETED_MARKER);
                    } else {
                        memTable.put(entry.getKey(), entry.getValue());
                    }
                } catch (IOException e) {
                    System.err.println("Skipping corrupted WAL entry: " + line);
                }
            }
        }
    }

    private void loadSSTables() throws IOException {
        try (Stream<Path> paths = Files.list(Paths.get(sstablePath))) {
            List<SSTableMeta> loadedMetas = new ArrayList<>();
            paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("sstable-"))
                    .forEach(path -> {
                        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                            String firstLine = reader.readLine();
                            if (firstLine != null) {
                                String minKey = firstLine.split(",", 2)[0];
                                String lastLine = firstLine;
                                String currentLine;
                                while ((currentLine = reader.readLine()) != null) {
                                    lastLine = currentLine;
                                }
                                String maxKey = lastLine.split(",", 2)[0];
                                loadedMetas.add(new SSTableMeta(path, minKey, maxKey));
                            }
                        } catch (IOException e) {
                            System.err.println("Could not read metadata for SSTable: " + path);
                        }
                    });
            Collections.sort(loadedMetas);
            this.sstables = new CopyOnWriteArrayList<>(loadedMetas);
        }
    }

    private void flushMemtableToSSTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        ConcurrentNavigableMap<String, String> entriesToFlush = memTable.getEntries();
        long timestamp = System.currentTimeMillis();
        Path sstableFile = Paths.get(sstablePath, "sstable-" + timestamp + ".txt");

        try (BufferedWriter writer = Files.newBufferedWriter(sstableFile, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, String> entry : entriesToFlush.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
        }

        SSTableMeta newMeta = new SSTableMeta(sstableFile, entriesToFlush.firstKey(), entriesToFlush.lastKey());
        sstables.add(0, newMeta);
        memTable.clear();
        walWriter.close();
        walWriter = Files.newBufferedWriter(Paths.get(walFilePath), StandardCharsets.UTF_8);
    }

    public void put(String key, String value) throws IOException {
        WalEntry entry = new WalEntry(key, value);
        walWriter.write(objectMapper.writeValueAsString(entry) + "\n");
        walWriter.flush();
        memTable.put(key, value);

        if (memTable.getSizeInBytes() > memtableMaxSize) {
            flushMemtableToSSTable();
        }
    }

    public void batchPut(List<KeyValue> keyValues) throws IOException {
        for (KeyValue kv : keyValues) {
            put(kv.getKey(), kv.getValue());
        }
    }

    public Optional<String> read(String key) throws IOException {
        String value = memTable.get(key);
        if (value != null) {
            if (value.equals(DELETED_MARKER)) {
                return Optional.empty();
            }
            return Optional.of(value);
        }

        for (SSTableMeta meta : sstables) {
            if (comparator.compare(key, meta.getMinKey()) < 0 || comparator.compare(key, meta.getMaxKey()) > 0) {
                continue;
            }
            try (BufferedReader reader = Files.newBufferedReader(meta.getPath(), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && parts[0].equals(key)) {
                        return parts[1].equals(DELETED_MARKER) ? Optional.empty() : Optional.of(parts[1]);
                    }
                }
            }
        }
        return Optional.empty();
    }

    public List<KeyValue> readKeyRange(String startKey, String endKey) throws IOException {
        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
        memTable.subMap(startKey, true, endKey, true)
                .forEach(mergedResults::put);
        for (SSTableMeta meta : sstables) {
            SSTableReader reader = new SSTableReader(meta.getPath(), new NaturalKeyComparator());
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
        memTable.put(key,DELETED_MARKER);
    }

    public List<SSTableMeta> getSstables() {
        return sstables;
    }

    public String getSstablePath() {
        return sstablePath;
    }

    public synchronized void replaceSSTables(List<SSTableMeta> oldMetas, SSTableMeta newMeta) {
        List<SSTableMeta> newSstableList = new ArrayList<>(sstables);
        newSstableList.removeAll(oldMetas);
        newSstableList.add(newMeta);
        Collections.sort(newSstableList); // Sorts newest-to-oldest
        this.sstables = new CopyOnWriteArrayList<>(newSstableList);
    }
}