package com.moniepoint.kvstore.controller;

import com.moniepoint.kvstore.service.KeyValueService;
import com.moniepoint.kvstore.pojo.KeyValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/kv")
public class KeyValueController {

    private static final Logger logger = LoggerFactory.getLogger(KeyValueController.class);

    @Autowired
    private KeyValueService service;

    @PutMapping("/{key}")
    public void put(@PathVariable String key, @RequestBody String value) throws IOException {
        logger.info("API: Received PUT request for key: {}", key);
        service.put(key, value);
    }

    @PutMapping("/batch")
    public void batchPut(@RequestBody List<KeyValue> keyValues) throws IOException {
        logger.info("API: Received BATCH PUT request for {} keys", keyValues.size());
        service.batchPut(keyValues);
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> read(@PathVariable String key) throws IOException {
        logger.info("API: Received GET request for key: {}", key);
        Optional<String> value = service.read(key);
        return value.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/range/{startKey}/{endKey}")
    public List<KeyValue> readKeyRange(@PathVariable String startKey, @PathVariable String endKey) throws IOException {
        logger.info("API: Received GET RANGE request from {} to {}", startKey, endKey);
        return service.readKeyRange(startKey, endKey);
    }

    @DeleteMapping("/{key}")
    public void delete(@PathVariable String key) throws IOException {
        logger.info("API: Received DELETE request for key: {}", key);
        service.delete(key);
    }
}
