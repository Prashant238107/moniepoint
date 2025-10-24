package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.entity.KeyValue;
import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
        List<Path> sstables = keyValueService.getSstables();
        if (sstables.size() < compactionFileCountTrigger) {
            return;
        }

        System.out.println("Starting compaction...");

        List<Path> toCompact = sstables.subList(sstables.size() - 1, sstables.size());

        ConcurrentNavigableMap<String, String> mergedResults = new ConcurrentSkipListMap<>(new NaturalKeyComparator());

        for (Path sstableFile : toCompact) {
            SSTableReader reader = new SSTableReader(sstableFile, new NaturalKeyComparator());
            List<KeyValue> sstableKVs = reader.readAll();
            for (KeyValue kv : sstableKVs) {
                mergedResults.put(kv.getKey(), kv.getValue());
            }
        }

        long timestamp = System.currentTimeMillis();
        Path compactedSSTable = Paths.get(keyValueService.getSstablePath(), "sstable-compacted-" + timestamp + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(compactedSSTable.toFile()))) {
            for (Map.Entry<String, String> entry : mergedResults.entrySet()) {
                if (!entry.getValue().equals(DELETED_MARKER)) {
                    writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                }
            }
        }
        keyValueService.replaceSSTables(toCompact, compactedSSTable);
        for (Path oldFile : toCompact) {
            Files.delete(oldFile);
        }
        System.out.println("Compaction finished.");
    }
}
