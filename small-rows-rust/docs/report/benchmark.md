# Benchmark

## Hardware Environment

- CPU: AMD Ryzen 9 5900X 12-core, 24-thread
- Memory: CORSAIR VENGEANCE LPX 64GB (2 x 32GB) DDR4 3200 (PC4-25600) C16
- SSD: Crucial P3 Plus 1TB PCIe Gen4 3D NAND NVMe M.2 SSD - up to 5000MB/s
- HDD: Seagate BarraCuda 4TB Internal Hard Drive HDD – 3.5 Inch Sata 6 Gb/s 5400 RPM

## Software Environment

- OS: Ubuntu 24.04 LTS (Linux 6.8.0-45-generic)

## Scenrio 1 - Concurrent Insert

- Insert 1,000,000 random records concurrently.
- Using 1 - 120 transactions, each transaction runs in a separate thread.
- Tuple schema: "(id bigint primary key, value bigint)".

### PostgreSQL

- server: PostgreSQL 17 (in Docker)
- client: 
