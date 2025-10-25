package com.moniepoint.kvstore.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class ClusterService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);

    private final List<String> configuredNodes;
    private final String currentNodeUrl;
    private final int replicationFactor;

    @Autowired
    private ReplicationClient replicationClient;

    private final Set<String> liveNodes = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void init() {
        liveNodes.addAll(configuredNodes);
    }

    public ClusterService(
            @Value("${kvstore.cluster.nodes}") List<String> nodes,
            @Value("${kvstore.cluster.current-node-url}") String currentNodeUrl,
            @Value("${kvstore.cluster.replication-factor}") int replicationFactor
    ) {
        this.configuredNodes = nodes;
        this.currentNodeUrl = currentNodeUrl;
        this.replicationFactor = replicationFactor;
        logger.info("ClusterService initialized. Current node: {}, All nodes: {}, Replication Factor: {}", currentNodeUrl, configuredNodes, replicationFactor);
    }

    public String getLeaderNodeForKey(String key) {
        List<String> replicas = getReplicaNodesForKey(key);
        for (String node : replicas) {
            if (liveNodes.contains(node)) {
                logger.debug("Leader for key '{}' is the first live replica: {}", key, node);
                return node;
            }
        }
        logger.error("CRITICAL: No live node found for key '{}'. Replicas were: {}", key, replicas);
        return null;
    }

    public boolean isCurrentNodeLeader(String key) {
        boolean isLeader = getLeaderNodeForKey(key).equals(currentNodeUrl);
        logger.debug("Current node {} is leader for key '{}': {}", currentNodeUrl, key, isLeader);
        return isLeader;
    }

    public String getCurrentNodeUrl() {
        return currentNodeUrl;
    }

    public List<String> getNodes() {
        return new ArrayList<>(liveNodes);
    }

    public List<String> getReplicaNodesForKey(String key) {
        List<String> replicas = new ArrayList<>();
        if (configuredNodes.isEmpty() || replicationFactor <= 0) {
            return replicas;
        }

        int startIndex = Math.abs(key.hashCode()) % configuredNodes.size();

        for (int i = 0; i < replicationFactor && i < configuredNodes.size(); i++) {
            int replicaIndex = (startIndex + i) % configuredNodes.size();
            replicas.add(configuredNodes.get(replicaIndex));
        }

        logger.debug("Replicas for key '{}' are: {}", key, replicas);
        return replicas;
    }

    @Scheduled(fixedRate = 10000, initialDelay = 15000)
    public void performHealthChecks() {
        logger.info("Performing cluster health checks...");
        for (String nodeUrl : configuredNodes) {
            if (nodeUrl.equals(currentNodeUrl)) continue;

            if (replicationClient.isNodeHealthy(nodeUrl)) {
                liveNodes.add(nodeUrl);
            } else {
                if (liveNodes.remove(nodeUrl)) {
                    logger.warn("Node {} is down. Removed from live set.", nodeUrl);
                }
            }
        }
        logger.info("Health check complete. Live nodes: {}", liveNodes);
    }
}