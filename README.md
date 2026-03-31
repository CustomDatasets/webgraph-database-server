# Common Crawl WebGraph Database Server & Clients, From CustomDatasets.com

**Project:** https://customdatasets.com
**Database:** https://customdatasets.com/webgraph/db/
**Documentation:** https://customdatasets.com/webgraph/db-docs/

Created by **Ben Wills**
https://benwills.com

---

## Overview

This repository contains server implementations and client libraries for working with the **CustomDatasets Common Crawl WebGraph database**.

The database contains the complete historical ranking data derived from the Common Crawl WebGraph and is designed for high-performance local querying.

The dataset includes:

* Domain ranking history
* Host ranking history
* PageRank metrics
* Harmonic Centrality metrics
* Ranking positions
* Historical crawl snapshots

Unlike the public API, the database allows:

* Unlimited queries
* No rate limits
* Local execution
* Full data access
* Custom analysis workflows

---

## Repository Structure

Server implementations:

* Go server
* PHP server (Swoole)
* Python server

Client implementations:

* Go client
* PHP client
* Python client

These implementations demonstrate how to:

* Locate database shards
* Query hostnames
* Parse ranking history
* Build high-performance lookup services
* Create internal APIs

---

## Database Structure

The database consists of SQLite files.

Data is divided into:

Domain databases:

```
domain.0.db → domain.31.db
```

Host databases:

```
host.0.db → host.31.db
```

Each hostname is assigned to a shard using a DJB2 hash of the reversed hostname.

This allows:

* Even distribution
* Fast lookup
* Horizontal scalability
* Predictable query routing

---

## Dataset Size

Approximate sizes:

Domains:

~300 GB

Hosts:

~850 GB

Combined:

~1.15 TB

Files are delivered compressed using Zstandard.

---

## Query Model

The database functions as a key-value store.

Lookup flow:

1 Determine shard
2 Query SQLite file
3 Retrieve TSV blob
4 Parse ranking history

Each lookup returns complete historical ranking records.

---

## PHP Server (Swoole)

The PHP implementation uses **Swoole** to create a highly concurrent server capable of handling large numbers of requests efficiently.

Benefits:

* Persistent workers
* No PHP process startup overhead
* Async I/O
* High throughput
* Memory efficiency

This allows PHP to behave more like Go or Node servers.

---

## When To Use This Database

Use the database if you need:

* Bulk analytics
* Data science pipelines
* Authority modeling
* Historical research
* Competitive analysis
* Local ranking services
* Internal APIs

Use the free API if you only need small lookups.

---

## Example Use Cases

Typical applications include:

SEO analysis:

Authority tracking across time.

Data science:

Historical ranking modeling.

Competitive research:

Comparing domains across crawl history.

Search research:

Link graph analysis.

Internal tools:

Authority lookup services.

---

## Installation Overview

Typical setup:

1 Download database files
2 Decompress Zstandard archives
3 Place SQLite shards on fast storage
4 Run server implementation
5 Connect using client libraries

Full documentation:

https://customdatasets.com/webgraph/db-docs/

---

## Performance Notes

Recommended:

NVMe storage

64GB+ RAM

Fast filesystem

Local execution is significantly faster than remote queries.

---

## About CustomDatasets

CustomDatasets builds production-grade datasets and data systems for marketing and engineering teams.

https://customdatasets.com

---

## About The Author

**Ben Wills**

https://benwills.com

Ben builds large-scale scraping systems, search infrastructure, and custom data products.

---

## License

MIT License recommended unless otherwise specified.

---

## Contributions

Potential improvements:

Additional server implementations

Benchmarking tools

Streaming query support

Index optimizations

Client improvements

---

## Related Resources

Database:

https://customdatasets.com/webgraph/db/

Database docs:

https://customdatasets.com/webgraph/db-docs/

API:

https://customdatasets.com/webgraph/api/

---

## Notes

This repository demonstrates how to build services on top of the dataset. It is not intended to be a packaged database product.

For production usage:

Add:

Authentication

Caching

Monitoring

Metrics

Retry logic

---

Built by Ben Wills.
