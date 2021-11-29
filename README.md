# Cloud Spanner Dialect for NHibernate

### Performance Recommendations
- Enable ADO.NET batching; set `adonet.batch_size` to a value higher than 1. A higher number means that more statements will be sent together as one RPC to Spanner.
- Avoid updating collections directly and modify the referencing column instead. That is: Do not set singer.Albums = { album1, album2, ... }, but call album1.Singer=singer, album2.Singer=singer, ...
- Configure entities to use dynamic updates (`dynamic-update=true`)
