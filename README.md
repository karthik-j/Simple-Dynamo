# Simple-Dynamo
*Distributed Systems CS586 - Spring 2015 class project-4, UB*

Replicated Key-Value Storage (Dynamo-style)
-------------------------------------------

In this assignment, I have implemented a Dynamo-style key-value storage. This assignment was about implementing a simplified version of Dynamo. (And you might argue that it’s not Dynamo any more ;-) There were three main pieces I had to implement: 1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal was to provide both availability and linearizability at the same time. In other words, the implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. We had freedom to come up with our own design as long as we were able to provide availability and linearizability at the same time (that is, to the extent that the tester can test). The exception was partitioning and replication, which should be done exactly the way Dynamo does.
