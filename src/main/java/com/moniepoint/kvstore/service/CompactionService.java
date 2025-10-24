package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.entity.KeyValue;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import com.moniepoint.kvstore.entity.SSTableMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

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

import static com.moniepoint.kvstore.constants.Constants.DELETED_MARKER;

@Service
public class CompactionService {

    @Autowired
    private KeyValueService keyValueService;

    @Value("${kvstore.compaction.trigger.file-count}")
    private int compactionFileCountTrigger;

    @Scheduled(fixedRateString = "${kvstore.compaction.schedule.ms}")
    public void runCompaction() throws IOException {
        List<SSTableMeta> sstables = keyValueService.getSstables();
        if (sstables.size() < compactionFileCountTrigger) {
            return;
        }

        List<SSTableMeta> toCompact = sstables.subList(sstables.size() - 2, sstables.size());
        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());

        for (SSTableMeta meta : toCompact) {
            SSTableReader reader = new SSTableReader(meta.getPath(), new NaturalKeyComparator());
            List<KeyValue> sstableKVs = reader.readAll();
            for (KeyValue kv : sstableKVs) {
                mergedResults.put(kv.getKey(), kv.getValue());
            }
        }

        long timestamp = System.currentTimeMillis();
        Path compactedSSTablePath = Paths.get(keyValueService.getSstablePath(), "sstable-" + timestamp + ".txt");

        try (BufferedWriter writer = Files.newBufferedWriter(compactedSSTablePath, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, String> entry : mergedResults.entrySet()) {
                if (!entry.getValue().equals(DELETED_MARKER)) {
                    writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                }
            }
        }

        SSTableMeta newMeta = new SSTableMeta(compactedSSTablePath, mergedResults.firstKey(), mergedResults.lastKey());
        keyValueService.replaceSSTables(toCompact, newMeta);

        for (SSTableMeta oldMeta : toCompact) {
            Files.delete(oldMeta.getPath());
        }
    }
}
