package com.moniepoint.kvstore.service;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class ReplicationClient {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationClient.class);
    private final RestTemplate restTemplate = new RestTemplate();

    public void forwardPut(String nodeUrl, String key, String value) {
        logger.info("Forwarding PUT request for key '{}' to node: {}", key, nodeUrl);
        restTemplate.put(nodeUrl + "/kv/" + key, value);
    }

    public void forwardDelete(String nodeUrl, String key) {
        logger.info("Forwarding DELETE request for key '{}' to node: {}", key, nodeUrl);
        restTemplate.delete(nodeUrl + "/kv/" + key);
    }
}