# Key-Value Datastore

A high-performance, persistent, and distributed key-value store built with Java and Spring Boot, inspired by the design of Log-Structured Merge-Tree (LSM-Tree) based systems like Apache Cassandra and Google's Bigtable.

## Core Features

- **LSM-Tree Architecture**: Optimized for high write throughput by converting random writes into sequential writes on disk.
- **Low-Latency Operations**: In-memory caching (`MemTable`) for fast reads and writes. SSTable metadata is used to minimize disk I/O for reads of older data.
- **High Throughput**: Utilizes concurrent data structures (`ConcurrentSkipListMap`, `CopyOnWriteArrayList`) to handle heavy access loads with minimal contention.
- **Crash Safety & Durability**: Implements a Write-Ahead Log (WAL) to ensure that no acknowledged writes are lost on a crash.
- **Fast Recovery**: The WAL is segmented and compacted, ensuring that server startup and recovery times remain fast and predictable.
- **Handles Large Datasets**: Manages datasets much larger than available RAM by flushing data from the in-memory `MemTable` to sorted on-disk files (`SSTables`).
- **Automatic Background Maintenance**:
    - **SSTable Compaction**: A scheduled service periodically merges smaller SSTables into larger ones to improve read performance and reclaim disk space from deleted or updated entries.
    - **WAL Cleanup**: A scheduled service cleans up obsolete WAL files whose data has been safely persisted in an SSTable.
- **Data Replication**: Ensures data durability by replicating writes to multiple nodes. It uses a leader-follower model with a configurable replication factor.
- **Automatic Failover (Simulated)**: The cluster can automatically detect node failures via health checks and elect a new leader for a data partition, ensuring high availability.
- **Distributed Request Routing**: Implements a leader-forwarding mechanism where nodes in a cluster automatically route write requests to the correct primary node.
- **RESTful API**: Provides simple and clean HTTP endpoints for all key-value operations, using JSON for request and response bodies.

## Architecture Overview

The system is built on two primary concepts: a local storage engine and a distributed coordination layer.

### Local Storage Engine (LSM-Tree)

1.  **Write-Ahead Log (WAL)**: All writes and deletes are first appended to a WAL file on disk for durability.
2.  **MemTable**: The data is then inserted into an in-memory sorted map (`ConcurrentSkipListMap`). This makes writes extremely fast.
3.  **SSTable (Sorted String Table)**: When the `MemTable` or WAL reaches a configured size limit, its contents are flushed to a new, immutable, sorted file on disk called an SSTable.
4.  **Compaction**: A background process merges multiple SSTables to consolidate data and remove deleted entries, keeping read performance optimal.

### Distributed Model

1.  **Partitioning**: The key space is partitioned across the nodes in the cluster using a simple hash-based algorithm. Each key has a designated "leader" node.
2.  **Request Forwarding**: When a node receives a write/delete request for a key it is not the leader for, it forwards the request to the correct leader node. This provides a single-system illusion to the client.
3.  **Replication and Failover**:
    - **Replication**: For each key, a set of replica nodes is determined. The first live node in this set acts as the **leader**. When the leader processes a write, it replicates the `WalEntry` to all other follower nodes in the set before confirming the write to the client.
    - **Health Checks & Failover**: Each node periodically runs health checks on its peers. If a leader node fails, it is removed from the "live" set. When a new request arrives for a key previously owned by the failed node, the system automatically promotes the next live replica in the preference list to be the new leader. This provides seamless, automatic failover.
    - **Coordination (Simulated)**: A `ClusterService` simulates a coordination service (like ZooKeeper or etcd) by managing the list of cluster members and their liveness state.

## Configuration

The application's behavior is configured in `src/main/resources/application.properties`.

### File Locations

By default, the application stores its data in a `data` directory relative to where it is run.

- **Write-Ahead Logs**: `kvstore.wal.path=data/wal`
- **SSTables**: `kvstore.sstable.path=data/sstables`

You can change these paths to absolute paths if desired (e.g., `/var/lib/kvstore/wal`).

### Key Settings

- `kvstore.memtable.max-size-bytes`: The size in bytes the `MemTable` can reach before triggering a flush to an SSTable.
- `kvstore.wal.max-size-bytes`: The maximum size a WAL file can reach before triggering a flush.
- `kvstore.compaction.trigger.file-count`: The number of SSTables that must exist before the compaction service will run.
- `kvstore.cluster.nodes`: A comma-separated list of all nodes in the cluster.
- `kvstore.cluster.replication-factor`: The total number of copies to keep for each piece of data (e.g., 3).
- `kvstore.cluster.current-node-url`: The URL of the specific node instance being run (used for self-identification).

## How to Run

### Prerequisites

- Java 24 or later
- Maven 3.x

### 1. Build the Application

Navigate to the project's root directory and build the executable JAR file using the Maven wrapper.

```sh
mvn clean install -DskipTests
```

This will create an executable JAR in the `target/` directory (e.g., `target/moniepoint-1.0-SNAPSHOT.jar`).

### 2. Running a Single Node

You can run a single instance of the key-value store with a simple command.

```sh
java -jar target/moniepoint-1.0-SNAPSHOT.jar
```

The server will start on `http://localhost:8080`.

### 3. Running a Multi-Node Cluster

To test the distributed features, you need to run multiple instances of the application, each on a different port and with a unique node URL.

Open **three separate terminal windows**. In each one, run one of the following commands.

**Terminal 1 (Node 0):**
```sh
java -jar target/moniepoint-1.0-SNAPSHOT.jar \
  --server.port=8080 \
  --kvstore.cluster.current-node-url=http://localhost:8080
```

**Terminal 2 (Node 1):**
```sh
java -jar target/moniepoint-1.0-SNAPSHOT.jar \
  --server.port=8081 \
  --kvstore.cluster.current-node-url=http://localhost:8081
```

**Terminal 3 (Node 2):**
```sh
java -jar target/moniepoint-1.0-SNAPSHOT.jar \
  --server.port=8082 \
  --kvstore.cluster.current-node-url=http://localhost:8082
```

You now have a 3-node cluster running locally. You can send requests to any node, and they will be automatically routed to the correct leader.

## API Endpoints

| Method | URL                               | Description                                      |
|--------|-------------------------------------------|--------------------------------------------------|
| `PUT`  | `/moniepoint/kv`                             | Creates or updates a value for the given key.    |
| `PUT`  | `/moniepoint/kv/batch`                       | Creates or updates multiple key-value pairs in bulk.     |
| `GET`  | `/moniepoint/kv/{key}`                       | Retrieves the value for the given key.           |
| `POST` | `/moniepoint/kv/range`                       | Retrieves all keys and values within a given range.    |
| `DELETE`| `/moniepoint/kv/{key}`                      | Deletes a key.                                   |

## Testing the Cluster

1.  **Start the 3-node cluster** as described above.
2.  **Send a write request to any node** (e.g., Node 8080).
    ```sh
    curl -X PUT -H "Content-Type: application/json" -d '{"key": "my-test-key", "value": "hello distributed world"}' http://localhost:8080/moniepoint/kv
    ```
3.  **Observe the logs**. You will see Node 8080 receive the request, determine the correct leader (e.g., Node 8082), and log that it is "Forwarding" the request. The logs on Node 8082 will show it received the request and processed it "locally".
4.  **Read the data back from the leader node**. You must query the node that stores the data.
    ```sh
    # Assuming Node 8082 is the leader for 'my-test-key'
    curl http://localhost:8082/moniepoint/kv/my-test-key
    ```
    This will return `hello distributed world`.