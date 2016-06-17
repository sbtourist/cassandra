/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.utils.SlidingTimeRate;

/**
 * The back-pressure state, tracked per replica host:
 */
class BackPressureInfo
{
    private final long backPressureWindowSize;
    final SlidingTimeRate incomingRate;
    final SlidingTimeRate outgoingRate;
    final RateLimiter outgoingLimiter;
    final AtomicBoolean overload;
    final AtomicLong lastCheck;
    final ReentrantLock lock;

    BackPressureInfo(long backPressureWindowSize)
    {
        this.backPressureWindowSize = backPressureWindowSize;
        this.incomingRate = new SlidingTimeRate(this.backPressureWindowSize, 100, TimeUnit.MILLISECONDS);
        this.outgoingRate = new SlidingTimeRate(this.backPressureWindowSize, 100, TimeUnit.MILLISECONDS);
        this.outgoingLimiter = RateLimiter.create(Double.MAX_VALUE);
        this.overload = new AtomicBoolean();
        this.lastCheck = new AtomicLong();
        this.lock = new ReentrantLock();
    }
}
