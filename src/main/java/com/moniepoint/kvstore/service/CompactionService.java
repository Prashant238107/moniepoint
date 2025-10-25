package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.pojo.KeyValue;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import com.moniepoint.kvstore.entity.SSTableMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.moniepoint.kvstore.constants.Constants.DELETED_MARKER;

@Service
public class CompactionService {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    @Autowired
    private KeyValueService keyValueService;

    @Value("${kvstore.compaction.trigger.file-count}")
    private int compactionFileCountTrigger;

    @Value("${kvstore.wal.path}")
    private String walPath;

    @Scheduled(fixedRateString = "${kvstore.compaction.schedule.ms}")
    public synchronized void runCompaction() throws IOException {
        logger.info("CompactionService: Starting scheduled SSTable compaction.");
        List<SSTableMetaData> sstables = keyValueService.getSstables();
        if (sstables.size() < compactionFileCountTrigger) {
            logger.debug("CompactionService: Not enough SSTables to trigger compaction ({} < {}). Skipping.", sstables.size(), compactionFileCountTrigger);
            return;
        }

        List<SSTableMetaData> toCompact = sstables.subList(Math.max(0, sstables.size() - 2), sstables.size());
        logger.info("CompactionService: Compacting {} SSTables: {}", toCompact.size(), toCompact.stream().map(m -> m.getPath().getFileName().toString()).collect(Collectors.joining(", ")));
        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());

        for (SSTableMetaData meta : toCompact) {
            SSTableReader reader = new SSTableReader(meta.getPath(), new NaturalKeyComparator());
            if (!Files.exists(meta.getPath())) {
                logger.warn("CompactionService: SSTable {} was deleted during compaction. Skipping.", meta.getPath().getFileName());
                continue;
            }
            List<KeyValue> sstableKVs = reader.readAll();
            for (KeyValue kv : sstableKVs) {
                mergedResults.put(kv.getKey(), kv.getValue());
            }
        }

        long timestamp = System.currentTimeMillis();
        Path compactedSSTablePath = Paths.get(keyValueService.getSstablePath(), "sstable-" + timestamp + ".txt");
        logger.debug("CompactionService: New compacted SSTable path: {}", compactedSSTablePath.getFileName());

        if (mergedResults.isEmpty()) {
            logger.info("CompactionService: Merged results are empty (all keys were tombstones or no data). Skipping creation of new SSTable.");
            return;
        }

        try (BufferedWriter writer = Files.newBufferedWriter(compactedSSTablePath, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, String> entry : mergedResults.entrySet()) {
                if (!entry.getValue().equals(DELETED_MARKER)) {
                    writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                }
            }
            logger.debug("CompactionService: Wrote {} entries to new SSTable.", mergedResults.size());
        }

        SSTableMetaData newMeta = new SSTableMetaData(compactedSSTablePath, mergedResults.firstKey(), mergedResults.lastKey());
        keyValueService.replaceSSTables(toCompact, newMeta);

        for (SSTableMetaData oldMeta : toCompact) {
            logger.debug("CompactionService: Deleting old SSTable: {}", oldMeta.getPath().getFileName());
            Files.delete(oldMeta.getPath());
        }
        logger.info("CompactionService: SSTable compaction finished. New SSTable: {}", newMeta.getPath().getFileName());
    }

    @Scheduled(fixedRateString = "${kvstore.compaction.schedule.ms}", initialDelay = 30000)
    public synchronized void compactWalFiles() throws IOException {
        logger.info("CompactionService: Starting scheduled WAL file compaction.");
        List<SSTableMetaData> sstables = keyValueService.getSstables();
        if (sstables.isEmpty()) {
            return;
        }

        SSTableMetaData oldestSSTable = sstables.get(sstables.size() - 1);
        logger.debug("CompactionService: Oldest SSTable is {}. Safe timestamp: {}", oldestSSTable.getPath().getFileName(), getTimestampFromPath(oldestSSTable.getPath()));
        long safeTimestamp = getTimestampFromPath(oldestSSTable.getPath());

        try (Stream<Path> walFiles = Files.list(Paths.get(walPath))) {
            walFiles
                    .filter(p -> p.getFileName().toString().startsWith("wal-"))
                    .filter(p -> getTimestampFromPath(p) < safeTimestamp)
                    .forEach(walFile -> {
                        try {
                            logger.info("CompactionService: Deleting obsolete WAL file: {}", walFile.getFileName());
                            Files.delete(walFile);
                        } catch (IOException e) {
                            logger.error("CompactionService: Failed to delete obsolete WAL file: {}", walFile.getFileName(), e);
                        }
                    });
        }
    }

    private long getTimestampFromPath(Path path) {
        String fileName = path.getFileName().toString();
        String timestampStr = fileName.replaceAll("[^0-9]", "");
        try {
            return Long.parseLong(timestampStr);
        } catch (NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }
}
