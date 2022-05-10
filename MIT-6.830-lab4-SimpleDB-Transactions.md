---
title: "MIT 6.830 Lab4 SimpleDB Transactions"
date: 2022-04-27T04:47:40+08:00
draft: false
---

## 简介

完成前三个lab，SimpleDB已经具有基本的CRUD的能力，但是还缺少事务。本lab需要实现一个简单的基于锁的事务管理系统。需要在代码适当的位置添加lock/unlock，并且track事务所持有的锁并在事务需要锁时授予它。

本lab使用严格两阶段锁（2PL）协议。在2PL协议下，每个transaction都会经过两个阶段：在第一个阶段里，transaction根据需求不断地获取锁，叫做 *growing phase (expanding phase)*；在第二个阶段里，transaction开始释放其持有的锁，根据2PL的规则，这个transaction不能再获得新的锁，它所持有的锁逐渐减少，叫做 *shrinking phase (contracting phase)*。而strict-2PL，则要求transaction在执行结束（commit/abort）时统一释放所有锁。这样避免了一个transaction abort后，其他transaction级联abort。例如：

```shell
tx A：lock page1，2，3 
tx A: unlock 3 
tx B: lock 3 // 基于tx A的更改执行事务
tx A: abort
tx B: abort // cascading abort
```

#### ACID保证

- Atomicity：Strict 2PL和细致的buffer管理保证原子性。
- Consistency：由于原子性的存在，数据库事务是一致的。SimpleDB不解决其他一致性问题（如key相关的一致性）。
- Isolation：Strict 2PL保证隔离性。
- Durability：强制的buffer管理机制保证持久性。

### Recovery and Buffer Management

为了简化工作，建议实现一个NO STEAL/FORCE buffer management policy。

- STEAL：是否允许未提交的transaction覆盖持久化存储中对象的最新提交值（对应SimpleDB，即是否允许在事务未commit时，将其相关的dirty page写入disk）。
- FORCE：允许事务提交前，是否要求其作出的修改都反映到持久化存储上（对应SimpleDB，即在事务commit前，是否要强制将其对应的所有dirty page写到disk）。

为了进一步简化工作，假设SimpleDB在执行`transactionComplete`操作时，不会crash。

上述三点意味着本lab不需要实现log-based recovery，因为不需要undo（不淘汰dirty page，即未提交的更改不会写入disk）；也不需要redo（commit时强制写出page到disk并且commit过程中不会crash）。

## Exercises

### 1. Granting Locks

在SimpleDB（例如`BufferPool`）中添加代码，允许调用者代表特定事务获取或者释放对象上的锁（shared/exclusive）。

实验建议在page的粒度上锁，SimpleDB的文档和单元都测试假定实现了page粒度的锁。

需要创建一些数据结构，跟踪每个事务持有哪些锁并在事务获取锁时进行检查，决定是否授予。

实现读写锁：

- 事务读之前，必须上shared lock。
- 事务写之前，必须上exclusive lock。
- 多个事务可以对同一个page上shared lock。
- 只能有一个事务对page上exclusive lock。
- 如果t是唯一持有page p上的shared lock的事务，那么可以升级为exclusive lock。

如果一个事务对锁的请求不能立即满足，应该阻塞，等到lock可用。一定要注意race condition。

我的实现包含一个`PageLockManager`类，所有锁的信息记录在一个`HashMap<PageId, HashMap<TransactionId, PageLockType>>`中，使用java内置的`synchronized`对其acquire/release方法进行同步。

### 2. Lock Lifetime

实现上述的strict-2PL。在之前的设计中，读取任何page都使用了`BufferPool.getPage()`，保证了*growing phase*。

为了保证*strict shrinking phase*，应该在事务commit/abort时一次性释放所有的锁。如果我们通过遍历bufferpool中当前的page，并判断是否加锁，然后释放的话，逻辑上是有bug的，因为被加锁的page有可能在执行过程中被evict了，所以要在`PageLockManager`中添加一个方法，用于释放指定事务上所有的锁。

此外，在执行过程中，2PL有一个例外。那就是对HeapFile插入时，查找有空slot的page，如果一个page上没有空slot，那么此时可以释放该page上的锁，然后继续查找其他page。这是因为插入时我们仅仅是查看该page有没有空slot而已，并没有其上任何信息

### 3. Implementing NO STEAL

No Steal策略保证了dirty page不会被evict，就保证了事务提交前，修改不会落盘，不需对disk文件回滚。只需要简单修改`evictPage`方法，不淘汰dirty page就好了，如果全是dirty page，就抛异常。

### 4. Transactions

在SimpleDB中，每个query开始时，创建一个`TransactionId`对象。这个对象被传递到所有在query中被调用的operator中。当query完成后，再调用BufferPool的`transactionComplete`方法。

在comit时，应当把BufferPool中跟事务相关的dirty page都flush；当事务终止，应该将与事务相关的dirty page都从disk读回，将BufferPool中的page变成事务执行前的状态。

这里逻辑很简单，但关于事务abort，我进了个坑，lab4的测试压力太小，没测出来，挂在lab5的`BTreeTest`上，查了两天的bug。

> 如果事务是在`insertTuple`/`deleteTuple`执行完之后再abort，那么由于no steal，abort时该事务的dirty page一定在BufferPool中被标记为dirty了，通过遍历BufferPool中的page，并将dirty page读回是可行的；但如果在执行过程中就abort了，那么BufferPool中被事务获取的page还没有标记dirty，我们需要通过`PageLockManager`来查看哪些page被加了exclusive锁，并把这些page从disk读回。

### 5. Deadlocks and Aborts

SimpleDB中的事务可能死锁，需要检测并且抛出`TransactionAbortedException`。

检测死锁的方式有很多种。最基本的例子是实现一个简单的超时策略，如果一个事务在给定的时间内还没有拿到需要的锁，就abort。而更实际的解决方案，可以基于等待图，定期检查依赖图中的环，或者当事务t acquire一个新的锁时，如果产生一个环，就中止某事务。如果终止t所等待的事务，可能会导致级联abort，但t能取得进展；如果终止t，那么其他事务可以正常运行。使用基于等待图的方案，需要修改`PageLockManager`的实现，所以我就实现了简单的超时策略。事务会不停地尝试获取一个锁，如果超时就abort。



