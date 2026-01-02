# [Programming for Data Science at Scale](https://opencourse.inf.ed.ac.uk/pdss) in [Scala](https://www.coursera.org/specializations/scala) with [Apache Spark](https://www.coursera.org/specializations/spark-python-big-data-analysis-pyspark)

## Big Data Paradigms

| MapReduce                             | Dataflow                     |
| ------------------------------------- | ---------------------------- |
| simpler, more constrained             | more complex data processing |
| Hadoop                                | Spark                        |
| Streaming: Storm, Flink               | Spark Streaming              |
| Query: Pig, Hive                      | Spark SQL                    |
| Hadoop Distributed File System (HDFS) | HDFS, S3, Cassandra          |

## Data-Parallel Programming

The [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) programming model: Everything is a key-value pair.

- `Map(key, value)`: `for each word in value: emit pair<word, +1>`
- `Reduce(key, list(values))`: `for each value in list(values): count += value; emit pair<word, count>`

This abstraction allows us to express the simple computations we were trying to perform but hides the messy details of _parallelization, fault-tolerance, data distribution and load balancing_ in a library. Our use of a functional model with userspecified map and reduce operations allows us to parallelize large computations easily and to use **re-execution as the primary mechanism for fault tolerance**.

The master is the conduit through which the location of intermediate file regions is propagated from map tasks to reduce tasks. Therefore, for each completed map task, the master stores the locations and sizes of the R intermediate file regions produced by the map task. Updates to this location and size information are received as map tasks are completed. The information is pushed incrementally to workers that have in-progress reduce tasks.

**Worker failure**: Any map task or reduce task in progress on a failed worker is also reset to idle and becomes eligible for rescheduling. Completed map tasks are re-executed on a failure because their output is stored on the local disk(s) of the failed machine and is therefore inaccessible. Completed reduce tasks do not need to be re-executed since their output is stored in a global file system.

**Network locality**: When running large MapReduce operations on a significant fraction of the workers in a cluster, most input data is read locally and consumes no network bandwidth.

MapReduce is weak for some requirements:

- Cannot define complex processes
- Everything file-based, no distributed memory
- Procedural -> difficult to optimize

Dataflow:

- Processing expressed as a DAG, tree, graph with
  cycles, ...
- Vertices: processing tasks
- Edges: Communication
  - DAG: Spark, Dryad
  - Tree: Dremel
  - Directed graph with cycles: Pregel
- Describing the processing tasks:
  - Declarative languages, e.g. Dremel
  - Functional programming, e.g. Spark - _Apache Spark and Scala Collections have a similar API_!
  - Domain-specific languages, e.g. Pregel for graph processing

**Fault tolerance** is essential for scaling out - and requires writing intermediate data to disk.

Spark:

- Data is _immutable and in-memory_.
- Operations are functional _transformations_.
- Fault tolerance is guaranteed through _replay operations_.

Compared to Hadoop, Spark improves efficiency through general execution graphs and in-memory storage. Spark is up to 10 x faster on disk, 100 x in memory.

## Functional Collections in Scala

Scala runs on the JVM and is also interoperable with JavaScript and C.

- Every value is an object.
- Every operation is a method call.
- Everything is an expression: No statements. No need for return and side-effects.
- Scala Classes behave exactly like Java classes.
- Scala Traits allow a form of multiple inheritance.

```scala
trait Ordered[A] extends java.lang.Comparable[A] {
  def <(that: A): Boolean = (this compareTo that) < 0
  def >(that: A): Boolean = (this compareTo that) > 0
  def <=(that: A): Boolean = (this compareTo that) <= 0
  def >=(that: A): Boolean = (this compareTo that) >= 0
}
case class Person(val name: String, val age: Int) extends Ordered[Person] {
  def compareTo(that: Person): Int =
    if (name < that.name) -1
    else if (name > that.name) 1
    else age - that.age
}
val p1 = new Person("anton", 10)
val p2 = new Person("berta", 5)
val p3 = new Person("anton", 9)
val ps = List(p1, p2, p3)
ps.sorted
```

FP:

- Referential transparency - no side effects
- Immutable variables - use `val` instead of `var`
- Immutable collections in the standard library
- Function literals - with syntatic sugar: `fun(args)` is desugared to `fun.apply(args)`, where `val fun = (x: Int) => x + 1`
- Higher-order functions - that take or return functions, and almost eliminate the need for loops over collections
- Many collections are functions:
  - `Seq[T]` is `Int => T`
  - `Set[T]` is `T => Boolean`
  - `Map[K,V]` is `K => V`

## Distributed Data-Parallel Programming

Although current frameworks provide numerous abstractions for accessing a cluster's computational resources, they lack abstractions for leveraging distributed memory. This makes them inefficient for an important class of emerging applications: those that reuse intermediate results across multiple computations. Data reuse is common in many iterative machine learning and graph algorithms, including PageRank, K-means clustering, and logistic regression. Another compelling use case is interactive data mining, where a user runs multiple ad-hoc queries on the same subset of the data. Unfortunately, in most current frameworks, the only way to reuse data between computations (e.g., between two MapReduce jobs) is to write it to an external stable storage system, e.g., a distributed file system. This incurs substantial overheads due to data replication, disk I/O, and serialization, which can dominate application execution times.

[**Resilient Distributed Datasets
(RDDs)**: _a fault-tolerant distributed memory abstraction for in-memory cluster computing_](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf) inspired by immutable Scala collections that enables efficient data reuse in a broad range of applications.

RDDs are **fault-tolerant, parallel** data structures that let users explicitly _persist_ intermediate results in memory, control their _partitioning_ to optimize data placement, and manipulate them using a rich set of _operators_ (most of which are higher-order functions: `map`, `flatMap`, `filter`, `reduce`, `fold`).

```scala
abstract class RDD[T] {
  def map[U](f: T => U): RDD[U] = ...
  def flatMap[U](f: T => TraversableOnce[U]): RDD[U] = ...
  def filter(p: T => Boolean): RDD[T] = ...
  def reduce(f: (T, T) => T): T = ...
}
```

The difference between Scala Collections and Spark RDDs: **distributed datasets**.

```scala
map[B](f: A=> B): List[B]  // Scala List
map[B](f: A=> B): RDD[B]  // Spark RDD
flatMap[B](f: A=> TraversableOnce[B]): List[B]  // Scala List
flatMap[B](f: A=> TraversableOnce[B]): RDD[B]  // Spark RDD
```

Formally, an RDD is a **read-only, partitioned collection of records**. RDDs can only be created through deterministic operations on either (1) data in stable storage or (2) other RDDs. We call these operations transformations to differentiate them from other operations on RDDs:
**Transformers** return new collections as results, not single values, e.g. `map`, `filter`, `flatMap`, `groupBy`, `distinct`. Transformers are _lazy_.
**Actions** compute a result using an RDD, and return the result or store it externally, e.g. `collect`, `count`, `take`, `reduce`. Actions are _eager_.

**Fault tolerance**: RDDs provide an interface based on coarse-grained transformations (e.g., `map`, `filter` and `join`) that apply the same operation to many data items. This allows them to efficiently provide fault tolerance by logging the transformations used to build a dataset (its **lineage**) rather than the actual data. If a partition of an RDD is lost, the RDD has enough information about how it was derived from other RDDs to recompute just that partition. Thus, lost data can be recovered, often quite quickly, without requiring costly replication.

| Aspect                     | RDDs                                        | Distr. Shared Memory                      |
| -------------------------- | ------------------------------------------- | ----------------------------------------- |
| Reads                      | Coarse- or fine-grained                     | Fine-grained                              |
| Writes                     | Coarse-grained                              | Fine-grained                              |
| Consistency                | Trivial (immutable)                         | Up to app / runtime                       |
| Fault recovery             | Fine-grained and low overhead using lineage | Requires checkpoints and program rollback |
| Straggler mitigation       | Possible using backup tasks                 | Difficult                                 |
| Work placement             | Automatic based on data locality            | Up to app (runtimes aim for transparency) |
| Behavior if not enough RAM | Similar to existing data flow systems       | Poor performance (swapping?)              |

The main difference between RDDs and DSM is that RDDs can only be created ("written") through coarse-grained transformations, while DSM allows reads and writes to each memory location. This restricts RDDs to applications that perform bulk writes, but allows for more efficient fault tolerance. In particular, RDDs do not need to incur the overhead of checkpointing, as they can be recovered using lineage. Furthermore, only the lost partitions of an RDD need to be recomputed upon failure, and they can be recomputed in parallel on different nodes, without having to roll back the whole program.

A second benefit of RDDs is that their immutable nature lets a system mitigate slow nodes (stragglers) by running backup copies of slow tasks as in MapReduce. Backup tasks would be hard to implement with DSM, as the two copies of a task would access the same memory locations and interfere with each other's updates.

Finally, RDDs provide two other benefits over DSM. First, in bulk operations on RDDs, a runtime can schedule tasks based on data locality to improve performance. Second, RDDs degrade gracefully when there is not enough memory to store them, as long as they are only being used in scan-based operations. Partitions that do not fit in RAM can be stored on disk and will provide similar performance to current data-parallel systems.

## Distributed Key-Value Processing

Single-node key-value pairs are dictionaries.

In big data processing distributed key-value pairs are used, to project down nested data types into key-value pairs.

For such **pair RDDs**, Spark automatically adds a number of useful additional methods: `groupByKey`, `reduceByKey`, `join`, `mapValues`, `countByKey`. They are most often created from existing non-pair RDDs (`val pairRDD: RDD[(String, String)] = rdd.map(p => (p.city, p.street))`), and once created pair-RDD-specific transformations can be used.

## Processing Sparse Data for large-scale sparse matrix operations

Store only the non-zero values and their locations.

Data structures:

- _Coordinate list (COO)_: simplest appraoch, a list of (row, col, value) triplets
  - Great for building a matrix: easy to append new non-zero entries
  - Terrible for computation: to find all elements in a row, you must scan the entire list
  - `O(nn)` storage complexity (where `nnz` is the number of non-zeros) - **perfect for data ingestion, terrible for computation -> CSR/CSC!**
  - Optimisation techniques:
    - _Row-Major Sorting_: better cache locality, faster access patterns!
    - _Memory Layout Optimisation_:
      - Structure of Arrays (SoA):
        - Store all rows together, then all columns, then all values
        - Better for vectorized operations on modern CPUs
      - Array of Structures (AoS):
        - Store (row, col, val) triplets together
        - Better for sequential access patterns
      - Hybrid Approach:
        - Use SoA for better vectorization in modern CPUs
        - Switch between formats based on operation type
    - _Block-Based COO_: group entries by blocks, for better cache utilisation, and for vectorised operations - used in high-performance libraries
    - _Compressed COO_: store only the differences between consecutive entries
- _Compressed Sparse Row (CSR)_: row offsets, column indices, values
  - **for row-based operations like row slicing and row-wise computations**
- _Compressed Sparse Column (CSC)_: column offsets, row indices, values
  - **for column-based operations like column slicing and column-wise computations**
- _Sliced Ellpack (SELL)_: improves the performance of problems involving low variability in the number of nonzero elements per row

_Build with COO, convert to CSR/CSC for computation, use SELL for GPU acceleration_.

| Feature       | COO      | CSR          | CSC          | SELL               |
| ------------- | -------- | ------------ | ------------ | ------------------ |
| Storage       | `O(nnz)` | `O(nnz + m)` | `O(nnz + n)` | `O(nnz + padding)` |
| Row access    | Slow     | Fast         | Slow         | Fast               |
| Column access | Slow     | Slow         | Fast         | Slow               |
| Modification  | Fast     | Very Slow    | Very Slow    | Very Slow          |
| Best for      | Building | Row ops      | Column ops   | GPU / Parallel     |

Matrix-Vector Multiplication:

|                    | Normal Matrix Multiplication | CSR Sparse Multiplication          |
| ------------------ | ---------------------------- | ---------------------------------- |
| Operations:        | `O(m x n)`                   | `O(nnz)`                           |
| Example:           | 1000 x 1000 matrix           | 1000 x 1000 matrix, 1000 non-zeros |
| Operations:        | 1,000,000                    | 1,000                              |
| Problem / Speedup: | Most are 0 x x[j] = 0        | 1000 x faster!                     |

**SpMV-CSR Algorithm**:

```text
For each row i from 0 to m − 1:
  y [i] = 0
  For k from row ptr[i] to row ptr[i+1]-1:
    y [i]+ = values[k] × x[col indices[k]]
```

Spark's Machine Learning library, MLlib, builds on these concepts to
handle sparse data efficiently at scale:

- RDDs: Low-level, flexible collections. We use transformations like `map`, `filter`, `reduceByKey`.
- Pair RDDs: RDDs of key-value pairs, enabling powerful operations like `join` and `groupByKey`.

`SparseVector` is Spark's primary way to represent a feature vector. It stores only non-zero values and their indices.

`SparseMatrix` represents a local matrix on a single machine. It is stored in CSC (Compressed Sparse Column) format - many ML operations like calculating statistics for a feature or updating a model weight are column-oriented.

A `CoordinateMatrix` is a distributed version of the COO format.

**SpGEMM = Sparse General Matrix-Matrix multiplication (C = AB)**: core operation in graph algorithms and collaborative filtering, but matrices A, B, C are too big for one machine

Solution Strategy:

1. Align: Group elements that will be multiplied together
2. Multiply: Compute products of matching pairs
3. Sum: Add all products for each output position

Key Takeaways:

1. Sparsity is everywhere. 99%+ zeros in NLP, recommender systems, graphs. Exploiting sparsity is essential for scalability.
2. Data structure = performance. COO for building, CSR for rows, CSC for columns. Choose based on access patterns.
3. Distributed sparse ops use map-reduce. SpGEMM shows how `map`, `join`, and `reduceByKey` enable scalable algorithms.

## Optimizing Distributed Data Processing

1. Hash Partitioning: How Spark assigns keys to nodes
2. Shuffle Internals: Map output, network, reduce input
3. Skew Problems: When some keys dominate
4. Optimizations: Broadcast joins, pre-partitioning

The `join` operation in SpGEMM is the most expensive step. _Shuffling_ moves data from one node to another to be grouped with its key. This is expensive due to network I/O (moving data between nodes), disk I/O (when data is too large to fit in memory), serialisation and deserialisation of data.

```scala
val totalWithdrawalsPerCustomer = withdrawals
  .map(w => (w.customerId, w.amount))
  .groupByKey()
  .mapValues(amounts => amounts.sum)
```

`reduceByKey` combines the steps of `groupByKey` and reduction into one
operation. Key Advantage: It performs _local aggregation_.

```scala
val totalWithdrawalsPerCustomer = withdrawals
  .map(w => (w.customerId, w.amount))
  .reduceByKey()
```

Where else shuffling?

1. `groupByKey()`: Spark needs to move all records with the same key to the same partition.
2. `reduceByKey()`: Shuffling occurs after local aggregation when Spark needs to move partial sums between partitions to calculate the final result.
3. `join()`: Spark must align keys from two RDDs.
4. `distinct()`: Ensure that duplicate records across partitions are compared and removed.
5. `sortByKey()`: Spark needs to globally sort data across all partitions.
6. `repartition()`: Redistributing data into a different number of partitions.

Grouping all values of key-value pairs with the same key requires collecting all key-value pairs with the same key on the same machine. But how does Spark know which key to put on which machine?

Key Properties of Partitions:

- Partitions never span multiple machines; all data in a partition stays on one machine.
- Each machine in the cluster contains one or more partitions.
- The number of partitions is configurable (default = total number of cores across executor nodes).

Types of Partitioning:

1. Hash Partitioning
   - Hashing customerId: Spark applies a hash function to customerId (e.g., 1, 2, 3) to determine the partition: `p = k.hashCode() % numPartitions`.
   - Partitioning: Data with the same hash value goes to the same partition. Different customerIds go to different partitions based on their hash.
   - Result: All records for a specific key are grouped into a single partition based on the hash function, ensuring efficient distribution.
2. Range Partitioning: Efficient for ordered data or when you need to process data within specific key ranges.

```scala
val hashpartitionedRDD = rdd.partitionBy(new HashPartitioner(numPartitions))

val rangePartitionedRDD = rdd.partitionBy(new RangePartitioner(numPartitions, rdd))

val automaticallyHashPartitionedByReduceOperationRDD = rdd.reduceByKey(_ + _)

val sortedRDDWithRangePartitionerUsedByDefault = rdd.sortByKey()
val groupedRDDWithHashPartitionerUsedByDefault = rdd.groupByKey()
```

After `partitionBy()`, Spark reshuffles and recomputes the entire RDD every time you perform an action, e.g. `count()`, `collect()`. Solution: `persist()` stores the RDD in memory (or disk) after the first computation.

```scala
val partitionedRDD = rdd.partitionBy(new HashPartitioner(100))

partitionedRdd.persist()
partitionedRdd.count()
partitionedRdd.collect()  // reuses the persisted data, no recomputation
```

_Partitioning can bring substantial performance gains, especially in the face of shuffles_.

## Distributed Query Processing

Spark vs. Hadoop MapReduce:

- More flexible programming model
- General execution graphs
- In-memory storage

Data spectrum:

- Unstructured: log files, images.
- Semi-structured: JSON, XML.
- Structured: database tables.

Spark RDDs don't know anything about
the data schema.

Everything about SQL is structured. Relational databases exploit these structures to get performance speedups.

It would be nice to _intermix SQL queries with Scala and get all the DB optimisations on Spark jobs_. [Spark SQL](https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf) delivers both, integrating relational processing (e.g., declarative queries and optimized storage) with Spark's functional programming API.

A **DataFrame** is the core abstraction of Spark SQL, equivalent to a table in a relational DB: `DataFrame = RDD + schema`. They can be created from RDDs by inferring (`val df = spark.createDataFrame(rowRDD)`) or explicitly specifying the schema (`val df = spark.createDataFrame(rowRDD, schema)`), or by reading a data source from file.

The **DataFrame API** is a relational API over Spark RDDs that can be **automatically aggressively optimised**.

**Catalyst**: Spark SQL's highly-extensible query optimizer

- Assumptions:
  - Has full knowledge of all data types
  - Knows the exact schema of our data
  - Has detailed knowledge of computations
- Optimizations:
  - Reordering operations
  - Reduce the amount of data read
  - Pruning unneeded partitioning

**Datasets** are a typed variant of DataFrames, enabling more typed operations and higher-order functions at the expense of Catalyst not being able to optimise these higher-order functional operations.

- Use datasets when
  - Structured/semi-structured data
  - Type-safety
  - Functional APIs
  - Good performance, but not the best
- Use DataFrames when
  - Structured/semi-structured data
  - Best possible performance, automatically optimized
- Use RDDs when
  - Unstructured/complex data
  - Fine-tune and manage low-level datails of RDD computations

## Distributed Graph Processing

Dependencies in graphs (e.g. PageRank) are difficult to express in MapReduce. The system is not optimised for iteration.

[Pregel](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf): **Bulk Synchronous Programming (BSP)** model

```scala
Class Vertex{
  //Main methods
  Compute(MessageIterator *msgs);
  SendMsgTo(dest, msg);
  VoteToHalt();

  //Auxiliary methods
  GetValue();
  MutableValue();
  GetOutEdgeIterator();
  SuperStep();
}
```

## Distributed Tensor Processing

A **tensor** is a generalization of matrices to arbitrary dimensions: scalars (0D), vectors (1D), matrices (2D), and higher-dimensional arrays (3D+). Deep learning models operate on tensors and require massive computational resources.

Challenges in distributed tensor training:

- **Model size**: Modern neural networks (GPT, BERT) have billions of parameters that don't fit on a single GPU
- **Data size**: Training datasets can be terabytes or petabytes
- **Computation**: Training can take weeks on a single machine
- **Memory constraints**: Gradients, activations, and optimizer states multiply memory requirements

Parallelization Strategies:

| Strategy          | Description                                    | Best For                       | Communication Overhead |
| ----------------- | ---------------------------------------------- | ------------------------------ | ---------------------- |
| Data Parallelism  | Replicate model, split data across workers     | Large datasets, small models   | Low (gradients only)   |
| Model Parallelism | Split model across workers, same data          | Large models, smaller datasets | High (activations)     |
| Pipeline          | Split model into stages, process mini-batches  | Very large models              | Medium                 |
| Tensor Parallel   | Split individual layers/tensors across workers | Huge layers (transformers)     | Very High              |
| Hybrid            | Combine multiple strategies                    | Massive scale training         | Varies                 |

**Data Parallelism**: Each worker has a complete copy of the model and processes a different subset of data.

```python
for batch in data_loader:
    # Each worker processes different batch
    loss = model(batch)
    gradients = compute_gradients(loss)

    # All-reduce: sum gradients across workers
    synchronized_gradients = all_reduce(gradients)

    # All workers update with same gradients
    optimizer.step(synchronized_gradients)
```

**Gradient Synchronization** is the bottleneck. Solutions:

- _All-Reduce_: Ring-based algorithms (e.g., Baidu's implementation) achieve bandwidth-optimal communication
- _Parameter Server_: Central servers manage parameters, but can become a bottleneck
- _Gradient Compression_: Reduce communication by sending sparse gradients

**Model Parallelism**: Split the model across multiple devices. Layer 1 on GPU 0, Layer 2 on GPU 1, etc. Problem: Poor utilization - GPUs sit idle waiting for activations.

**Pipeline Parallelism**: Divide model into stages and process multiple mini-batches simultaneously. Each stage works on a different mini-batch, reducing idle time through pipelining.

Key Frameworks:

- **PyTorch Distributed**: `DistributedDataParallel` (DDP) for data parallelism, `RPC` for model parallelism
- **TensorFlow**: `tf.distribute.Strategy` API for various parallelization strategies
- **Horovod**: Framework-agnostic distributed training using MPI and NCCL
- **DeepSpeed**: Microsoft's library for training extremely large models with ZeRO (Zero Redundancy Optimizer)
- **Megatron-LM**: NVIDIA's framework for training trillion-parameter models

Key Takeaways:

1. **Choose the right strategy**: Data parallelism for most cases, model parallelism when models don't fit on one device.
2. **Communication is critical**: Network bandwidth often limits scaling. Optimize gradient synchronization.
3. **Fault tolerance matters**: Long-running jobs need checkpointing. Save model state regularly.
4. **Mixed precision training**: Use FP16/BF16 to reduce memory and increase throughput without sacrificing accuracy.

**Tensor Algebra**: Matricised Tensor Times Khatri-Rao Product (MTTKRP) is to tensors what SpMV (Sparse Matrix-Vector multiplication) is to matrices - a core computational kernel. Given a tensor and factor matrices, MTTKRP computes the product along one mode while performing a Khatri-Rao product on the other modes' factors.
