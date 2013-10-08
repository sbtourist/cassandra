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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.utils.WrappedRunnable;

import org.junit.Test;

import static org.junit.Assert.*;

public class BatchingThreadPoolExecutorTest
{
    @Test
    public void testBatchExecution() throws InterruptedException
    {
        final BatchingThreadPoolExecutor executor = new BatchingThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(),
                new NamedThreadFactory("TEST"));

        final int times = 1000;
        final Future[] futures = new Future[times];
        final CountDownLatch executionLatch = new CountDownLatch(times);
        final AtomicInteger executionCounter = new AtomicInteger(0);
        final Runnable task = new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                executionLatch.countDown();
                executionCounter.incrementAndGet();
            }
        };

        for (int i = 0; i < times; i++)
        {
            futures[i] = executor.submit(task);
        }

        assertTrue(executionLatch.await(1, TimeUnit.MINUTES));
        assertEquals(times, executionCounter.get());
        for (int i = 0; i < times; i++)
        {
            assertTrue(futures[i].isDone());
        }
    }

    @Test
    public void testBatchChangesAfterRun() throws InterruptedException
    {
        BatchingThreadPoolExecutor executor = new BatchingThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(),
                new NamedThreadFactory("TEST"));

        final CountDownLatch longRunningStart = new CountDownLatch(1);
        Runnable longRunningTask = new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                longRunningStart.countDown();
                Thread.sleep(3000);
            }
        };
        Runnable shortRunningTask = new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(10);
            }
        };

        executor.submit(longRunningTask);

        RunnableBatch initialBatch = executor.getCurrentBatch().get();

        assertTrue(longRunningStart.await(1, TimeUnit.MINUTES));

        executor.submit(shortRunningTask);
        assertNotSame(initialBatch, executor.getCurrentBatch().get());

        RunnableBatch accumulatedBatch = executor.getCurrentBatch().get();
        executor.submit(shortRunningTask);
        assertSame(accumulatedBatch, executor.getCurrentBatch().get());
    }

    @Test
    public void testConcurrentBatchesAreExecutedOnlyOnce() throws InterruptedException
    {
        final BatchingThreadPoolExecutor executor = new BatchingThreadPoolExecutor(10, 10,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(),
                new NamedThreadFactory("TEST"));

        final int times = 1000;
        final CountDownLatch executionLatch = new CountDownLatch(times);
        final AtomicInteger executionCounter = new AtomicInteger(0);
        final Runnable task = new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                Thread.sleep(10);
                executionLatch.countDown();
                executionCounter.incrementAndGet();
            }
        };

        ExecutorService runner = Executors.newFixedThreadPool(10);
        for (int i = 0; i < times; i++)
        {
            runner.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    executor.submit(task);
                }
            });
        }

        assertTrue(executionLatch.await(1, TimeUnit.MINUTES));
        assertEquals(times, executionCounter.get());
    }
}
