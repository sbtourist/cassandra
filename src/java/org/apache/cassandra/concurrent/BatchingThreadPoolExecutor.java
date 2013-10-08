/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.cassandra.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ThreadPoolExecutor implementation based on batching, for improved throughput.
 *
 * For performance reasons, it provides the following deviations from standard
 * ThreadPoolExecutor behavior:
 *
 * 1. If the work queue is bounded, the bound will be the number of batches, not
 * the actual number of submitted tasks.
 *
 * 2. The getTaskCount() method is not implemented.
 *
 * 3. The getCompletedTaskCount() method is not implemented.
 *
 * Number #1 is the most relevant: future versions may be able to overcome it in
 * optimized ways.
 */
public class BatchingThreadPoolExecutor extends ThreadPoolExecutor
{
    private final AtomicReference<RunnableBatch> currentBatch = new AtomicReference<RunnableBatch>();

    public BatchingThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue)
    {
        super(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue);
    }

    public BatchingThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory)
    {
        super(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public BatchingThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler)
    {
        super(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public BatchingThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            RejectedExecutionHandler handler)
    {
        super(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    public void execute(Runnable task)
    {
        RunnableBatch current = currentBatch.get();
        while (current == null || !current.batch(task))
        {
            RunnableBatch candidate = new RunnableBatch();
            if (currentBatch.compareAndSet(current, candidate))
            {
                // A new batch from the current thread can be enqueued with no additional checks:
                candidate.batch(task);
                super.execute(candidate);
                break;
            }
            // A new batch coming from another thread may already be in run mode, so loop again:
            current = currentBatch.get();
        }
    }

    @Override
    public long getTaskCount()
    {
        throw new UnsupportedOperationException("Unsupported for performance reasons.");
    }

    @Override
    public long getCompletedTaskCount()
    {
        throw new UnsupportedOperationException("Unsupported for performance reasons.");
    }

    protected final AtomicReference<RunnableBatch> getCurrentBatch()
    {
        return currentBatch;
    }
    
}
