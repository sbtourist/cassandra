package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private static volatile MessagingService messagingService;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setBackPressureStrategy("org.apache.cassandra.net.MessagingServiceTest$MockBackPressureStrategy()");
        messagingService = MessagingService.test();
    }

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.destroyConnectionPool(InetAddress.getLocalHost());
    }
    
    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testOnlyTracksBackPressureWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MessageIn<String> message = MessageIn.create(InetAddress.getLocalHost(), "", Collections.emptyMap(), MessagingService.Verb.MUTATION, MessagingService.current_version);
        BackPressureInfo backPressureInfo = messagingService.getConnectionPool(message.from).getBackPressureInfo();
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.maybeTrackBackPressure(message, noCallback);
        assertEquals(0.0, backPressureInfo.incomingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.maybeTrackBackPressure(message, bpCallback);
        assertEquals(0.0, backPressureInfo.incomingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.maybeTrackBackPressure(message, bpCallback);
        assertEquals(1.0, backPressureInfo.incomingRate.get(TimeUnit.SECONDS), 0.0);
    }

    @Test
    public void testOnlyAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        MessageIn<String> message = MessageIn.create(InetAddress.getLocalHost(), "", Collections.emptyMap(), MessagingService.Verb.MUTATION, MessagingService.current_version);
        BackPressureInfo backPressureInfo = messagingService.getConnectionPool(message.from).getBackPressureInfo();
        
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(message.from);
        assertFalse(MockBackPressureStrategy.applied);
        assertEquals(0.0, backPressureInfo.outgoingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(message.from);
        assertTrue(MockBackPressureStrategy.applied);
        assertEquals(1.0, backPressureInfo.outgoingRate.get(TimeUnit.SECONDS), 0.0);
    }
    
    @Test
    public void testDoesntIncrementOutgoingRateWhenOverloaded() throws UnknownHostException
    {
        MessageIn<String> message = MessageIn.create(InetAddress.getLocalHost(), "", Collections.emptyMap(), MessagingService.Verb.MUTATION, MessagingService.current_version);
        BackPressureInfo backPressureInfo = messagingService.getConnectionPool(message.from).getBackPressureInfo();
        
        backPressureInfo.overload.set(true);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(message.from);
        assertTrue(MockBackPressureStrategy.applied);
        assertEquals(0.0, backPressureInfo.outgoingRate.get(TimeUnit.SECONDS), 0.0);
    }
    
    public static class MockBackPressureStrategy implements BackPressureStrategy
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(String[] args)
        {
        }

        @Override
        public void apply(BackPressureInfo backPressureInfo)
        {
            applied = true;
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
}
