## G-thinker
G-thinker is a user-friendly system for parallel graph mining

### What is special about G-thinker?
Existing Big Data frameworks are dominantly **data-intensive** where communication caused by data movement is the bottleneck, and CPU cores are underutilized. Solving **compute-intensive** graph mining problems using these frameworks often results in even worse performance.

G-thinker fundamentally changes the data-intensive design of existing Big Data frameworks. It adopts a **task-based** execution engine that is able to **keep CPUs in a cluster fully utilized** even with a budget limit on memory consumption. It also provides a **subgraph-centric API** that is natural for writing graph mining algorithms. G-thinker is orders of magnitude faster than existing systems and scales to graphs orders of magnitude larger given the same hardware environment.

### Documentation
We maintain detailed documentation in our awesome system website: https://yanlab19870714.github.io/yanda/gthinker/.

### Contact
UAB Data Lab (or YanLab@UAB): https://yanlab19870714.github.io/yanda/
Email: yanda@uab.edu
