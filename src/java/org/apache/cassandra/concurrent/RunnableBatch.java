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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runnable implementation with internal batching capabilities: in a typical
 * task-submission scenario, producers can batch runnable objects which will be
 * collected and executed in FIFO order.
 *
 * This class is completely thread safe and concurrent, implemented with a
 * lock-free approach based on CAS.
 *
 * The algorithm basically works this way:
 *
 * 1. The number of concurrent in-flight batch operations is kept on an atomic
 * variable: threads can always try to batch, but will actually succeed only if
 * not in run mode.
 *
 * 2. Once the run method is called, run mode is entered: new batches will not
 * be accepted anymore, and in-flight batches will be waited for to complete; to
 * maximize actual CPU utilization, already batched tasks are executed while
 * waiting (so it's not a real wait), and finally remaining tasks are executed.
 *
 */
public class RunnableBatch implements Runnable
{
    private final NBQueue tasks = new NBQueue();
    private final AtomicInteger batches = new AtomicInteger(0);
    private final AtomicBoolean run = new AtomicBoolean(false);

    public boolean batch(Runnable task)
    {
        // First check for run mode (so to avoid starvation if entering when already in run mode):
        if (!run.get())
        {
            // Start batch block and keep track of in-flight batches (to be waited for during run mode):
            batches.incrementAndGet();
            try
            {
                // Second check for run mode, offer only if not in run mode 
                // (*needed* to avoid offering after the queue has been already processed):
                if (!run.get())
                {
                    tasks.push(task);
                    return true;
                }
                return false;
            }
            finally
            {
                batches.decrementAndGet();
            }
        }
        return false;
    }

    @Override
    public void run()
    {
        // Enter run mode, to prevent new batches to be executed
        if (run.compareAndSet(false, true))
        {
            // While waiting for in-flight batches to complete, do some useful work and start dequeuing:
            while (batches.get() > 0)
            {
                Runnable task = tasks.pop();
                if (task != null)
                {
                    task.run();
                }
            }
            // After all batches are done, we can be sure no more tasks will be enqueued as we're in run mode, so finish dequeuing:
            Runnable task = tasks.pop();
            while (task != null)
            {
                task.run();
                task = tasks.pop();
            }
        }
        else
        {
            throw new IllegalStateException("Cannot run more than one time!");
        }
    }

    private static class NBQueue
    {
        private final Node stub = new Node();
        private final AtomicReference<Node> head = new AtomicReference<Node>(stub);
        private volatile Node tail = stub;

        public void push(Runnable obj)
        {
            Node candidate = new Node(obj);
            Node prev = head.getAndSet(candidate);
            prev.next = candidate;
        }

        public Runnable pop()
        {
            if (tail.next != null)
            {
                Runnable result = tail.next.state;
                tail = tail.next;
                return result;
            }
            else
            {
                return null;
            }
        }

        private static class Node
        {
            public final Runnable state;
            public volatile Node next;

            public Node()
            {
                state = null;
            }

            public Node(Runnable state)
            {
                this.state = state;
            }
        }
    }
}
