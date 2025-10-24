package com.moniepoint.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.pojo.KeyValue;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.moniepoint.kvstore.constants.Constants.DELETED_MARKER;

@Service
public class KeyValueService {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueService.class);

    @Value("${kvstore.wal.path}")
    private String walPath;

    @Value("${kvstore.sstable.path}")
    private String sstablePath;

    @Value("${kvstore.memtable.max-size-bytes}")
    private long memtableMaxSize;

    @Value("${kvstore.wal.max-size-bytes}")
    private long walMaxSize;

    @Autowired
    private MemTable memTable;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ReplicationClient replicationClient;

    @Autowired
    private ObjectMapper objectMapper;
    private BufferedWriter walWriter;
    private Path currentWalFile;
    private long currentWalSize = 0;
    private List<SSTableMeta> sstables = new CopyOnWriteArrayList<>();
    private final NaturalKeyComparator comparator = new NaturalKeyComparator();

    @PostConstruct
    public void init() throws IOException {
        Path sstableDir = Paths.get(sstablePath);
        if (!Files.exists(sstableDir)) {
            Files.createDirectories(sstableDir);
        }
        Path walDir = Paths.get(walPath);
        if (!Files.exists(walDir)) {
            Files.createDirectories(walDir);
        }

        loadSSTables();
        recoverState();
        rotateWalFile();
        logger.info("KeyValueService initialized. Current node URL: {}", clusterService.getCurrentNodeUrl());
    }

    @PreDestroy
    public void shutdown() throws IOException {
        flushMemtableToSSTable();
        if (walWriter != null) {
            walWriter.close();
        }
        logger.info("KeyValueService shutdown complete.");
    }

    private void recoverState() throws IOException {
        try (Stream<Path> walFiles = Files.list(Paths.get(walPath))
                .filter(p -> p.getFileName().toString().startsWith("wal-"))
                .sorted()) {

            logger.info("Recovering state from WAL files in: {}", walPath);
            walFiles.forEach(walFile -> {
                try (Stream<String> lines = Files.lines(walFile, StandardCharsets.UTF_8)) {
                    lines.forEach(line -> {
                try {
                    WalEntry entry = objectMapper.readValue(line, WalEntry.class);
                    if (entry.isDeleted()) {
                        memTable.put(entry.getKey(), DELETED_MARKER);
                    } else {
                        memTable.put(entry.getKey(), entry.getValue());
                        logger.debug("Recovered WAL entry: key={}, value={}", entry.getKey(), entry.getValue());
                    }
                } catch (IOException e) {
                    logger.error("Skipping corrupted WAL entry: {}", line, e);
                        }
                        logger.debug("MemTable size after recovery: {} bytes", memTable.getSizeInBytes());
                    });
                } catch (IOException e) {
                    logger.error("Could not read WAL file: {}", walFile, e);
                }
            });

        }
    }

    private void loadSSTables() throws IOException {
        try (Stream<Path> paths = Files.list(Paths.get(sstablePath))) {
            logger.info("Loading SSTable metadata from: {}", sstablePath);
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
                                SSTableMeta meta = new SSTableMeta(path, minKey, maxKey);
                                loadedMetas.add(meta);
                                logger.debug("Loaded SSTable: {} (minKey={}, maxKey={})", path.getFileName(), minKey, maxKey);
                            }
                        } catch (IOException e) {
                            logger.error("Could not read metadata for SSTable: {}", path, e);
                        }
                    });
            Collections.sort(loadedMetas);
            this.sstables = new CopyOnWriteArrayList<>(loadedMetas);
        }
    }

    private synchronized void flushMemtableToSSTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }
        logger.info("Flushing MemTable ({} bytes) to SSTable.", memTable.getSizeInBytes());

        ConcurrentNavigableMap<String, String> entriesToFlush = memTable.getEntries();
        long timestamp = System.currentTimeMillis();
        Path sstableFile = Paths.get(sstablePath, "sstable-" + timestamp + ".txt");

        try (BufferedWriter writer = Files.newBufferedWriter(sstableFile, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, String> entry : entriesToFlush.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                logger.debug("Flushed entry to SSTable: key={}", entry.getKey());
            }
        }

        SSTableMeta newMeta = new SSTableMeta(sstableFile, entriesToFlush.firstKey(), entriesToFlush.lastKey());
        sstables.add(0, newMeta);
        memTable.clear();
        logger.info("MemTable flushed to SSTable: {} (minKey={}, maxKey={}). MemTable cleared.", sstableFile.getFileName(), newMeta.getMinKey(), newMeta.getMaxKey());

        Path oldWalFile = this.currentWalFile;
        rotateWalFile();
        if (oldWalFile != null) {
            Files.delete(oldWalFile);
            logger.info("Deleted old WAL file: {}", oldWalFile.getFileName());
        }
    }

    private void rotateWalFile() throws IOException {
        if (walWriter != null) {
            walWriter.close();
        }
        this.currentWalFile = Paths.get(walPath, "wal-" + System.currentTimeMillis() + ".log");
        logger.info("Rotating WAL file. New WAL file: {}", this.currentWalFile.getFileName());
        this.currentWalSize = 0;
        this.walWriter = Files.newBufferedWriter(currentWalFile, StandardCharsets.UTF_8);
    }

    public void put(String key, String value) throws IOException {
        if (!clusterService.isCurrentNodeLeader(key)) {
            String leaderNode = clusterService.getLeaderNodeForKey(key);
            logger.info("PUT for key '{}' is not for this node. Forwarding to leader: {}", key, leaderNode);
            replicationClient.forwardPut(leaderNode, key, value);
            return;
        }
        logger.info("PUT for key '{}' processed locally (this node is leader).", key);

        WalEntry entry = new WalEntry(key, value);
        writeToWalAndCheckLimits(entry);
        memTable.put(key, value);
    }

    private void writeToWalAndCheckLimits(WalEntry entry) throws IOException {
        String walLine = objectMapper.writeValueAsString(entry) + "\n";
        walWriter.write(walLine);
        logger.debug("Wrote to WAL: {}", walLine.trim());
        walWriter.flush();
        currentWalSize += walLine.getBytes(StandardCharsets.UTF_8).length;

        if (memTable.getSizeInBytes() > memtableMaxSize || currentWalSize > walMaxSize) {
            flushMemtableToSSTable();
        }
    }

    public void batchPut(List<KeyValue> keyValues) throws IOException {
        for (KeyValue kv : keyValues) {
            logger.debug("Processing batch PUT for key: {}", kv.getKey());
            put(kv.getKey(), kv.getValue());
        }
    }

    public Optional<String> read(String key) throws IOException {
        if (!clusterService.isCurrentNodeLeader(key)) {
            logger.warn("READ for key '{}' received by non-leader node. This indicates a mis-routed request or stale data.", key);
        }
        logger.info("READ for key '{}' processed locally.", key);

        String value = memTable.get(key);
        if (value != null) {
            if (value.equals(DELETED_MARKER)) {
                return Optional.empty();
            }
            return Optional.of(value);
        } else {
            logger.debug("Key '{}' not found in MemTable. Checking SSTables.", key);
        }

        for (SSTableMeta meta : sstables) {
            logger.debug("Checking SSTable: {} (minKey={}, maxKey={}) for key '{}'", meta.getPath().getFileName(), meta.getMinKey(), meta.getMaxKey(), key);
            if (comparator.compare(key, meta.getMinKey()) < 0 || comparator.compare(key, meta.getMaxKey()) > 0) {
                logger.debug("Key '{}' is outside range of SSTable {}. Skipping.", key, meta.getPath().getFileName());
                continue;
            }
            try (BufferedReader reader = Files.newBufferedReader(meta.getPath(), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && parts[0].equals(key)) { // Found key in SSTable
                        return parts[1].equals(DELETED_MARKER) ? Optional.empty() : Optional.of(parts[1]);
                    }
                }
            }
        }
        return Optional.empty();
    }

    public List<KeyValue> readKeyRange(String startKey, String endKey) throws IOException {
        logger.info("READ RANGE from '{}' to '{}' processed locally.", startKey, endKey);
        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
        memTable.subMap(startKey, true, endKey, true)
                .forEach(mergedResults::put);
        for (SSTableMeta meta : sstables) {
            logger.debug("Checking SSTable: {} for range [{}, {}]", meta.getPath().getFileName(), startKey, endKey);
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
        // 1. Determine the leader node for this key
        if (!clusterService.isCurrentNodeLeader(key)) {
            String leaderNode = clusterService.getLeaderNodeForKey(key);
            logger.info("DELETE for key '{}' is not for this node. Forwarding to leader: {}", key, leaderNode);
            replicationClient.forwardDelete(leaderNode, key);
            return;
        }
        logger.info("DELETE for key '{}' processed locally (this node is leader).", key);

        WalEntry entry = new WalEntry(key, null, true);
        writeToWalAndCheckLimits(entry);
        memTable.put(key,DELETED_MARKER);
        logger.debug("Key '{}' marked for deletion in MemTable.", key);
    }

    public List<SSTableMeta> getSstables() {
        return sstables;
    }

    public String getCurrentNodeUrl() { return clusterService.getCurrentNodeUrl(); }

    public String getSstablePath() {
        return sstablePath;
    }

    public synchronized void replaceSSTables(List<SSTableMeta> oldMetas, SSTableMeta newMeta) {
        logger.info("Replacing {} old SSTables with new SSTable: {}", oldMetas.size(), newMeta.getPath().getFileName());
        List<SSTableMeta> newSstableList = new ArrayList<>(sstables);
        newSstableList.removeAll(oldMetas);
        newSstableList.add(newMeta);
        Collections.sort(newSstableList); // Sorts newest-to-oldest
        this.sstables = new CopyOnWriteArrayList<>(newSstableList);
    }
}