# `local-k3s-resilience`

This overlay bases on [`../local-k3s-full`](../local-k3s-full) and replaces the
`emptyDir` volumes for Postgres, Kafka, and Cassandra with `local-path`
PersistentVolumeClaims.

What that means:

- PVC-backed storage survives a pod restart on the same local cluster
- Kafka is still **single-broker**
- Cassandra is still **single-node**
- this is still **not HA** and **not failover**
- local-path storage is a workstation convenience, not a production storage
  claim

Supported claim boundary:

- pod restarts can preserve local state well enough to validate restart/recovery
  behavior on a single local cluster

Unsupported claim boundary:

- production durability
- multi-node resilience
- storage replication
- broker failover
- physical node loss recovery
