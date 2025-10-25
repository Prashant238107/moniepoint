package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.pojo.KeyValue;
import com.moniepoint.kvstore.pojo.RangeRequest;
import com.moniepoint.kvstore.entity.WalEntry;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Component
public class ReplicationClient {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationClient.class);
    private final RestTemplate restTemplate = new RestTemplate();

    public void forwardPut(String nodeUrl, String key, String value) {
        logger.info("Forwarding PUT request for key '{}' to node: {}", key, nodeUrl);
        KeyValue request = new KeyValue(key, value);
        restTemplate.put(nodeUrl + "/moniepoint/kv", request);
    }

    public void forwardDelete(String nodeUrl, String key) {
        logger.info("Forwarding DELETE request for key '{}' to node: {}", key, nodeUrl);
        restTemplate.delete(nodeUrl + "/moniepoint/kv/" + key);
    }

    public List<KeyValue> forwardRangeQuery(String nodeUrl, RangeRequest request) {
        String url = nodeUrl + "/moniepoint/kv/range?internal=true";
        logger.info("Forwarding RANGE request to node: {}", url);
        ResponseEntity<List<KeyValue>> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(request),
                new ParameterizedTypeReference<>() {}
        );
        return response.getBody();
    }

    public void replicateWalEntry(String nodeUrl, WalEntry entry) {
        logger.info("Replicating WAL entry for key '{}' to node: {}", entry.getKey(), nodeUrl);
        restTemplate.put(nodeUrl + "/moniepoint/kv/internal/replicate", entry);
    }

    public boolean isNodeHealthy(String nodeUrl) {
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(nodeUrl + "/moniepoint/kv/internal/health", String.class);
            return response.getStatusCode() == HttpStatus.OK;
        } catch (Exception e) {
            logger.warn("Health check failed for node: {}. Reason: {}", nodeUrl, e.getMessage());
            return false;
        }
    }
}