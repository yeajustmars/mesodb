# MesoDB

MesoDB is a Datalog query engine written on top of [DataFusion](). It turns Datalog queries into DataFusion's logical plan, and then executes the plan against an Apache Arrow/Apache Parquet database. MesoDB is HTAP (Hybrid Transactional/Analytical Processing) database, which means it can handle both transactional and analytical workloads. It is designed to be pretty fast, scalable, and easy to use.

# Dev

## Testing

### Unit tests

```
# cd /path/to/mesodb
cargo test
```

### Benchmarks

```
# cd /path/to/mesodb
cargo bench -p mesodb-bench --bench ingestion --bench query
```

