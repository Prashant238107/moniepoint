package com.moniepoint.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.entity.KeyValue;
import com.moniepoint.kvstore.entity.WalEntry;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
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
    private List<Path> sstables = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() throws IOException {
        Path sstableDir = Paths.get(sstablePath);
        if (!Files.exists(sstableDir)) {
            Files.createDirectories(sstableDir);
        }
        walWriter = new BufferedWriter(new FileWriter(walFilePath, true));
        recoverState();
        loadSSTables();
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
            sstables = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("sstable-"))
                    .sorted(Collections.reverseOrder()) // Newest first
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }
    }

    private void flushMemtableToSSTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        long timestamp = System.currentTimeMillis();
        Path sstableFile = Paths.get(sstablePath, "sstable-" + timestamp + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sstableFile.toFile()))) {
            for (Map.Entry<String, String> entry : memTable.getEntries().entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
        }

        sstables.add(0, sstableFile);
        memTable.clear();
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
        for (Path sstableFile : sstables) {
            try (BufferedReader reader = new BufferedReader(new FileReader(sstableFile.toFile()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && parts[0].equals(key)) {
                        if (parts[1].equals(DELETED_MARKER)) {
                            return Optional.empty();
                        } else {
                            return Optional.of(parts[1]);
                        }
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
        memTable.put(key,DELETED_MARKER); // Use marker for tombstone
    }

    public List<Path> getSstables() {
        return sstables;
    }

    public String getSstablePath() {
        return sstablePath;
    }

    public synchronized void replaceSSTables(List<Path> oldSSTables, Path newSSTable) {
        List<Path> newSstableList = new ArrayList<>(sstables);
        newSstableList.removeAll(oldSSTables);
        newSstableList.add(newSSTable);
        newSstableList.sort(Collections.reverseOrder());
        this.sstables = new CopyOnWriteArrayList<>(newSstableList);
    }
}