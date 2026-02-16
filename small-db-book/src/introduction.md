# Introduction

small-db is a distributed SQL database written in C++20. It is built as an educational project to explore the internals of geo-distributed database systems.

## Key Features

- **PostgreSQL wire protocol** -- Connect using standard PostgreSQL clients (`psql`, drivers, etc.)
- **LIST-based partitioning** -- Partition tables across regions by column values
- **Gossip-based replication** -- Cross-region data replication via a gossip protocol
- **RocksDB storage** -- Persistent key-value storage with MVCC (multi-version concurrency control)
- **gRPC coordination** -- Catalog updates, query forwarding, and replication over gRPC
