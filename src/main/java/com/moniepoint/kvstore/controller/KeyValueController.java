package com.moniepoint.kvstore.controller;

import com.moniepoint.kvstore.service.KeyValueService;
import com.moniepoint.kvstore.entity.KeyValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/kv")
public class KeyValueController {

    @Autowired
    private KeyValueService service;

    @PutMapping("/{key}")
    public void put(@PathVariable String key, @RequestBody String value) throws IOException {
        service.put(key, value);
    }

    @PutMapping("/batch")
    public void batchPut(@RequestBody List<KeyValue> keyValues) throws IOException {
        service.batchPut(keyValues);
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> read(@PathVariable String key) throws IOException {
        Optional<String> value = service.read(key);
        return value.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/range/{startKey}/{endKey}")
    public List<KeyValue> readKeyRange(@PathVariable String startKey, @PathVariable String endKey) throws IOException {
        return service.readKeyRange(startKey, endKey);
    }

    @DeleteMapping("/{key}")
    public void delete(@PathVariable String key) throws IOException {
        service.delete(key);
    }
}
