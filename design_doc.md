# Design Document

## Overview

Modern OLAPs are designed to perform analytic queries, with which we often need to do subqueries, joins, and aggregation. The problem is that the query optimizer needs metadata and statistics, such as data distribution and indexes to do a better job. To solve this problem, we need to build a catalog that serves as a “database of metadata for the database” that stores statistics from the execution engine and data discovery from I/O services, and that provides metadata to the planner and data location to schedule the query.

## Architectural Design

![Archirectural Diagram](https://github.com/teyenc/15721_catalog_private/assets/56297484/706e834b-9983-4de2-ad28-2c0e4acf3fd2)

#### Input/Output

We will be exposing a REST API for interaction with other components. The inputs and results for the same are a part of the API Spec.

#### Components

The components of our architecture include the rust service application and rocksDB as an embedded database.

##### Rust Application

A Rust application that exposes a REST API to modify database metadata of the OLAP database. The application consists of the following components:

- A data model that defines the structure and meaning of the metadata we store, such as schemas, tables, columns, measures, dimensions, hierarchies, and levels. The data model is represented by Rust structs that can be serialized and deserialized using Substrait.
- A database layer that interacts with RocksDB. The database layer provides methods for storing and retrieving the database metadata as key-value pairs using the RocksDB crate.
- A service layer that contains the business logic for the REST API, such as validating inputs, checking permissions, handling errors, etc. The service layer depends on the database layer and uses the data model to manipulate the database metadata.
- A controller layer that exposes the service methods as RESTful endpoints using a web framework, such as warp or axum. The controller layer uses the web framework’s features, such as filters, macros, and async functions, to parse the request parameters and format the response.

##### Database for metadata

We choose RocksDB as the database in the catalog to store metadata. It is a fast and persistent key-value store that can be used as an embedded database for Rust applications.

##### RocksDB Schema

Column Families

We use column families for a logical separation and grouping of the metadata store. The

- Table Data
  - Name - string (key)
  - Number of columns - u64
  - Read properties - json
  - Write properties - json
  - File URLs array - string array
  - Columns arrays, for each column
    - Aggregates - json object
    - Range of values - int pair
      - Lower bound
      - Upper bound
    - name - string
    - isStrongKey - boolean
    - isWeakKey - boolean
  - Primary Key col name
- Namespace Data
  - Name - string (key)
  - Properties - json object
- Operator statistics
  - Operator string - string (key)
  - Cardinality of prev result - u64

#### Tuning/Configuration options

The catalog can be passed a configuration file at bootstrap with the following configuration options:

1. data-warehouse-location
2. client-pool-size
3. cache-enabled
4. cache-expiration-interval

## Design Rationale

An explanation of why you chose the given design. Your justification should discuss issues related to (1) correctness, (2) performance, (3) engineering complexity/maintainability, and (4) testing. It should also include a brief discussion of the other implementations that you considered and why they were deemed inferior.
Most design decisions were made with the assumption that we do not have any schema updates and writes are infrequent with bulk data

#### Database

We contemplated two embedded database candidates for catalog service: SQLite and RocksDB. We chose RocksDB because

1. Better concurrency control: SQLite locks the entire database when dealing with concurrency writing, whereas RocksDB supports snapshots.
2. Flexibility: RocksDB provides more configuration options.
3. Scalability: RocksDB stores data in different partitions, whereas SQLite stores data in one single file, which isn’t ideal for scalability.
4. Storage: RocksDB uses key-value storage, which is good for intensive write operations. In short, RocksDB would provide better performance and concurrency control when we deal with write-intensive workloads.

#### Why a key-value store?

1. Based on [1], the catalog for an OLAP system behaves a lot like an OLTP database. They state how using a key-value store in the form of FoundationDB has proved to be beneficial based on performance and scalability. This includes supporting high-frequency reads, and support for dynamic metadata storage.
2. [2] compares and benchmarks the performance of tabular storage vs hierarchical organization of metadata as seen in Iceberg and finds the single node processing in Iceberg performs better than the others for small tables but fails to scale. It concludes that the metadata access patterns have a significant impact on the performance of distributed query processing.
3. Taking these factors into account, we have decided to go ahead with a key-value store for the simplicity and flexibility it provides along with the performance benefits.

#### Axum

After looking through several available options to use build APIs, such as Hyper and Actix, we have selected Axum.

- Axum framework is built on top of Hyper and Tokio and abstracts some of the low level details
- This, however, does not result in any significant performance overhead.
- Benchmarks for frameworks are listed in [3]

## Testing plan

A detailed description of how you are going to determine that your implementation is both (1) correct and (2) performant. You should describe the short unit tests and long running regression tests. Some portion of your testing plan must also use your project's public API, thus you are allowed to share testing infrastructure with the other group implementing the same thing.

### Correctness testing

#### Unit tests

For the correctness of the catalog, we plan to conduct unit tests and regression tests. In unit testing, we will test key components and operations such as metadata retrieval, metadata storage, update, and snapshot isolation.

Basic unit tests for handler functions are underway.

#### Regression tests

Currently, we plan to conduct regression tests on

1. Concurrency and parallelism to ensure data integrity
2. Correctness of all the APIs in API spec documentation.

### Performance testing

We plan to run the TPC-DS benchmark against the Iceberg API. Since we have a similar API as the Iceberg catalog, we can replace the Iceberg catalog with our service and run benchmark tests for comparison and performance benchmarking.

## Trade-offs and Potential Problems

Describe any conscious trade-off you made in your implementation that could be problematic in the future or any problems discovered during the design process that remain unaddressed (technical debts).
The biggest trade-off being made in the current design is the absence of any optimizations for updates. Updates to any tables will result in the metadata of the tables stored to become stale. Efficiently updating these values is a design challenge. This has not been prioritized based on the assumption that updates in an OLAP system will be infrequent.
Organizing the namespaces and tables as prefixes in the keys for the store may cause problems in terms of maintainability.
Database
We chose RocksDB to store metadata, whereas Iceberg Catalog has its own metadata layer that includes metadata files, manifest lists, and manifests. Using RocksDB could be more straightforward to implement compared to building everything from scratch. The components in Iceberg Catalog are likely to be optimized for Iceberg Catalog, and they could outperform RocksDB, which is not dedicated to catalog service.

### Milestones

- 75%: Basic API support
- 100%: Support for parallelism and performance tuning
- 125%: Performance comparable to or better than Iceberg Catalog and MVCC

### Task distribution

- Simran: Data access and organization
- Aditya: Rest API implementation

### References

[1] https://www.snowflake.com/blog/how-foundationdb-powers-snowflake-metadata-forward/
[2] https://15721.courses.cs.cmu.edu/spring2024/papers/18-databricks/p92-jain.pdf
[3] https://github.com/programatik29/rust-web-benchmarks/blob/master/result/hello-world.md
