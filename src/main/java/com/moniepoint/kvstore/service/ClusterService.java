package com.moniepoint.kvstore.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Service
public class ClusterService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);

    private final List<String> nodes;
    private final String currentNodeUrl;

    public ClusterService(
            @Value("${kvstore.cluster.nodes}") List<String> nodes,
            @Value("${kvstore.cluster.current-node-url}") String currentNodeUrl
    ) {
        this.nodes = nodes;
        this.currentNodeUrl = currentNodeUrl;
        logger.info("ClusterService initialized. Current node: {}, All nodes: {}", currentNodeUrl, nodes);
    }

    public String getLeaderNodeForKey(String key) {
        if (nodes.isEmpty()) {
            return currentNodeUrl;
        }
        int partition = Math.abs(key.hashCode()) % nodes.size();
        String leader = nodes.get(partition);
        logger.debug("Key '{}' hashes to partition {} (leader: {})", key, partition, leader);
        return leader;
    }

    public boolean isCurrentNodeLeader(String key) {
        boolean isLeader = getLeaderNodeForKey(key).equals(currentNodeUrl);
        logger.debug("Current node {} is leader for key '{}': {}", currentNodeUrl, key, isLeader);
        return isLeader;
    }

    public String getCurrentNodeUrl() {
        return currentNodeUrl;
    }
}