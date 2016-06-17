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

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Back-pressure algorithm based on the ration between incoming and outgoing rates, and low/high watermarks.
 */
public class RateBasedBackPressure implements BackPressureStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(RateBasedBackPressure.class);
    private final double highRatio;
    private final double lowRatio;
    private final int factor;

    public RateBasedBackPressure(double highRatio, double lowRatio, int factor)
    {
        if (highRatio <= 0 || highRatio > 1)
            throw new IllegalArgumentException("Back-pressure high ratio must be > 0 and <= 1");
        if (lowRatio <= 0 || lowRatio > 1)
            throw new IllegalArgumentException("Back-pressure low ratio must be > 0 and <= 1");
        if (highRatio <= lowRatio)
            throw new IllegalArgumentException("Back-pressure low ratio must be smaller than high ratio");
        if (factor < 1)
            throw new IllegalArgumentException("Back-pressure factor must be >= 1");
        this.highRatio = highRatio;
        this.lowRatio = lowRatio;
        this.factor = factor;
    }

    @Override
    public boolean apply(BackPressureInfo backPressure)
    {
        // Increase outgoing rate:
        backPressure.outgoingRate.update(1);

        RateLimiter limiter = backPressure.outgoingLimiter;
        long backPressureWindowHalfSize = backPressure.windowSize / 2;

        long lastCheck = backPressure.lastCheck.get();
        long interval = backPressure.windowSize;
        long now = System.currentTimeMillis();

        // Check/Update the back-pressure state at a given interval and only on a single thread:
        if ((now - lastCheck > interval) && backPressure.lock.tryLock())
        {
            try
            {
                // Get the incoming/outgoing rates by only looking into half the window size: we do not consider the 
                // full window so that we don't get biased by most recent outgoing requests that didn't receive
                // an incoming response *yet*; by looking at the first half of the window, we get instead a good
                // approximate measure of how many requests we sent and how many we've got back.
                double incomingRate = backPressure.incomingRate.get(
                        TimeUnit.SECONDS.convert(backPressureWindowHalfSize, TimeUnit.MILLISECONDS),
                        TimeUnit.SECONDS);
                double outgoingRate = backPressure.outgoingRate.get(
                        TimeUnit.SECONDS.convert(backPressureWindowHalfSize, TimeUnit.MILLISECONDS),
                        TimeUnit.SECONDS);

                // Now compute the incoming/outgoing ratio:
                double actualRatio = outgoingRate > 0 ? incomingRate / outgoingRate : 1;
                // If the ratio is above the high mark, try growing by the back-pressure factor:
                if (actualRatio >= highRatio)
                {
                    double newRate = limiter.getRate() + ((limiter.getRate() * factor) / 100);
                    if (newRate > 0)
                        limiter.setRate(newRate);
                    
                    backPressure.overload.set(false);
                }
                // If in between low and high marks, set the rate limiter at the incoming rate, but reduced by
                // the back-pressure factor to make it sustainable:
                else if (actualRatio >= lowRatio && actualRatio < highRatio)
                {
                    limiter.setRate(incomingRate - ((incomingRate * factor) / 100));
                    
                    backPressure.overload.set(false);
                }
                // Otherwise if previously overloaded, clear the overload flag (we don't want to flood the
                // client with errors) and try limiting at a very reduced rate:
                else if (backPressure.overload.get())
                {
                    limiter.setRate(outgoingRate * lowRatio);
                    
                    backPressure.overload.set(false);
                }
                // Finally if just below the low ratio, set the overload flag:
                else
                {
                    backPressure.overload.set(true);
                }

                // Housekeeping: pruning windows and resetting the last check timestamp!
                backPressure.incomingRate.prune();
                backPressure.outgoingRate.prune();
                backPressure.lastCheck.set(now);

                logger.debug("Back-pressure enabled with: incoming rate {}, outgoing rate {}, ratio {}, rate limiting {}",
                             incomingRate, outgoingRate, actualRatio, limiter.getRate());
            }
            finally
            {
                backPressure.lock.unlock();
            }
        }
        // If overloaded, signal back by returning true!
        if (backPressure.overload.get())
            return true;
        
        // Otherwise rate limit:
        limiter.acquire(1);
        return false;
    }

    public static class Factory implements BackPressureStrategy.Factory
    {
        private final String[] args;

        public Factory(String[] args)
        {
            if (args.length != 3)
            {
                throw new IllegalArgumentException(RateBasedBackPressure.class.getCanonicalName()
                        + " requires 3 arguments: high ratio, low ratio, back-pressure factor.");
            }

            this.args = args;
        }

        @Override
        public BackPressureStrategy make()
        {
            return new RateBasedBackPressure(Double.parseDouble(args[0].trim()),
                                             Double.parseDouble(args[1].trim()),
                                             Integer.parseInt(args[2].trim()));
        }
    }
}
