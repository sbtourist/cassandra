/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.utils.SlidingTimeRate;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

/**
 * The back-pressure state, tracked per replica host.
 */
class BackPressureInfo
{
    final long windowSize;
    final SlidingTimeRate incomingRate;
    final SlidingTimeRate outgoingRate;
    final RateLimiter outgoingLimiter;
    final AtomicBoolean overload;
    final AtomicLong lastCheck;
    final ReentrantLock lock;
    
    BackPressureInfo(long windowSize)
    {
        this(new SystemTimeSource(), windowSize);
    }

    @VisibleForTesting
    BackPressureInfo(TimeSource timeSource, long windowSize)
    {
        this.windowSize = windowSize;
        this.incomingRate = new SlidingTimeRate(timeSource, this.windowSize, 100, TimeUnit.MILLISECONDS);
        this.outgoingRate = new SlidingTimeRate(timeSource, this.windowSize, 100, TimeUnit.MILLISECONDS);
        this.outgoingLimiter = RateLimiter.create(Double.POSITIVE_INFINITY);
        this.overload = new AtomicBoolean();
        this.lastCheck = new AtomicLong();
        this.lock = new ReentrantLock();
    }
}
