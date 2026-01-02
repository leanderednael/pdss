# Project

How do we multiply matrices with billions of entries that won't fit on one machine? Especially when they are sparse?

Implement a distributed engine for the following fundamental large-scale sparse matrix and tensor operations in a distributed environment using Apache Spark:

| Operation                                                   | Left Operand  | Right Operand |
| ----------------------------------------------------------- | ------------- | ------------- |
| Sparse Matrix-Vector Multiplication (SpMV): $y = A \cdot x$ | Sparse Matrix | Dense Vector  |
| Sparse Matrix-Vector Multiplication (SpMV): $y = A \cdot x$ | Sparse Matrix | Sparse Vector |
| Sparse Matrix-Matrix Multiplication (SpMM): $C = A \cdot B$ | Sparse Matrix | Dense Matrix  |
| Sparse Matrix-Matrix Multiplication (SpMM): $C = A \cdot B$ | Sparse Matrix | Sparse Matrix |
| Tensor Algebra (e.g., MTTKRP)                               |               |               |

**Components**: The frontend is the user-facing API for specifying an operation and handles different operand types (sparse & dense). The execution engine manages data representation and implements the distributed algorithm.

```mermaid
graph LR
    A[User Request] -->|e.g. C = A * B| B[Frontend]
    B --> C[Execution Engine]
    C --> D[Distributed Storage]
    D --> C
```

**Data Layout**: A matrix is a 2D structure, but RDDs are a flat collection of records. How do you bridge this gap? Your choice of data layout is the single most important decision you will make.

- Loading mechanisms
- Dense representations: matrix, vector
- Sparse representations: matrix, vector

**Multiplication Logic**: Implement the core logic by decomposing the distributed tensor operation into many small, independent calculations that can be run in parallel across multiple workers, using Spark's distributed data-parallel RDD operations like `map`, `flatMap`, `groupByKey` and especially `join` - without any unnecessary `collect()`, which would bring data to the driver for computation.

A naive implementation will create excessive data shuffling between workers. The solution is intelligent Data Partitioning:

- Hashing strategies for optimal data distribution
- Partitioning schemes to minimize data movement

**Advanced Data Layout**: Then research and consider more advanced layouts:

- CSR, DCSR: very efficient for SpMV.
- CSF: for tensor algebra.

**Algebraic Optimisations**: Can you use mathematical properties to reduce the amount of computation or communication?

- For example, in a chain of multiplications, ($A \cdot B \cdot C$), the order matters!
- Other algabraic optimization like distributivity: $A \cdot (B + C ) = A \cdot B + A \cdot C$.
