package com.moniepoint.kvstore.controller;

import com.moniepoint.kvstore.service.KeyValueService;
import com.moniepoint.kvstore.pojo.KeyValue;
import com.moniepoint.kvstore.entity.WalEntry;
import com.moniepoint.kvstore.pojo.RangeRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/moniepoint/kv")
public class KeyValueController {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueController.class);

    @Autowired
    private KeyValueService service;

    @PutMapping
    public ResponseEntity<Void> put(@RequestBody KeyValue request) throws IOException {
        logger.info("API: Received PUT request for key: {}", request.getKey());
        boolean created = service.put(request.getKey(), request.getValue());
        if (created) {
            return ResponseEntity.status(201).build();
        } else {
            return ResponseEntity.ok().build();
        }
    }

    @PutMapping("/batch")
    public void batchPut(@RequestBody List<KeyValue> keyValues) throws IOException {
        logger.info("API: Received BATCH PUT request for {} keys", keyValues.size());
        service.batchPut(keyValues);
    }

    @PutMapping("/internal/replicate")
    public void replicate(@RequestBody WalEntry entry) throws IOException {
        logger.info("API: Received internal replication request for key: {}", entry.getKey());
        service.applyReplicatedWalEntry(entry);
    }

    @GetMapping("/internal/health")
    public ResponseEntity<Void> healthCheck() {
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{key}")
    public ResponseEntity<KeyValue> read(@PathVariable String key) throws IOException {
        logger.info("API: Received GET request for key: {}", key);
        Optional<String> value = service.read(key);
        return value.map(v -> ResponseEntity.ok(new KeyValue(key, v)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/range")
    public List<KeyValue> readKeyRange(
            @RequestBody RangeRequest request,
            @RequestParam(required = false, defaultValue = "false") boolean internal
    ) throws IOException {
        logger.info("API: Received POST RANGE request from {} to {} (internal: {})", request.getStartKey(), request.getEndKey(), internal);
        return service.readKeyRange(request.getStartKey(), request.getEndKey(), internal);
    }

    @DeleteMapping("/{key}")
    public void delete(@PathVariable String key) throws IOException {
        logger.info("API: Received DELETE request for key: {}", key);
        service.delete(key);
    }
}
