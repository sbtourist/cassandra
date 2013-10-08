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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

public class ThreadPoolExecutorPerfTest
{
    // TODO: Some code duplication here, obviously could be refactored...
    @Test
    public void testStandardThroughput() throws InterruptedException
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                q,
                new NamedThreadFactory("STANDARD"));

        System.out.println("Standard test...");

        System.out.println("Warming up...");

        runTest(executor, 1, 100000);

        System.out.println("Testing...");

        long start = System.currentTimeMillis();

        runTest(executor, 1, 1000000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("Elapsed ms for testing phase (no warm up): " + elapsed);
    }

    @Test
    public void testStandardThroughputUnderContention() throws InterruptedException
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                q,
                new NamedThreadFactory("STANDARD"));

        System.out.println("Standard test under contention...");

        System.out.println("Warming up...");

        runTest(executor, 1, 100000);

        System.out.println("Testing...");

        long start = System.currentTimeMillis();

        runTest(executor, 100, 1000000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("Elapsed ms for testing phase (no warm up): " + elapsed);
    }

    @Test
    public void testBatchingThroughput() throws InterruptedException
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        BatchingThreadPoolExecutor executor = new BatchingThreadPoolExecutor(1, 1,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                q,
                new NamedThreadFactory("BATCHING"));

        System.out.println("Batching test...");

        System.out.println("Warming up...");

        runTest(executor, 1, 100000);

        System.out.println("Testing...");

        long start = System.currentTimeMillis();

        runTest(executor, 1, 1000000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("Elapsed ms for testing phase (no warm up): " + elapsed);
    }

    @Test
    public void testBatchingThroughputUnderContention() throws InterruptedException
    {
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
        BatchingThreadPoolExecutor executor = new BatchingThreadPoolExecutor(10, 10,
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS,
                q,
                new NamedThreadFactory("STANDARD"));

        System.out.println("Batching test under contention...");

        System.out.println("Warming up...");

        runTest(executor, 1, 100000);

        System.out.println("Testing...");

        long start = System.currentTimeMillis();

        runTest(executor, 250, 1000000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("Elapsed ms for testing phase (no warm up): " + elapsed);
    }

    private void runTest(final ThreadPoolExecutor executor, final int producers, final int times) throws InterruptedException
    {
        final CountDownLatch counter = new CountDownLatch(times);
        final Runnable task = new Runnable()
        {
            @Override
            public void run()
            {
                counter.countDown();
            }
        };

        List<Thread> threads = new ArrayList<Thread>(producers);
        for (int i = 0; i < producers; i++)
        {
            threads.add(new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    for (int i = 0; i < times / producers; i++)
                    {
                        executor.submit(task);
                    }
                }
            }));
        }
        for (Thread producer : threads)
        {
            producer.start();
        }

        // Use a latch to be sure they're actually executed!
        assertTrue(counter.await(1, TimeUnit.MINUTES));
    }
}
