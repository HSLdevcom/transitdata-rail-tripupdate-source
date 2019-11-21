package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FeedEntityPublisherTest {
    private static GtfsRealtime.FeedMessage FEEDMESSAGE = null;
    private static FeedEntityPublisher feedEntityPublisher = null;

    @Before
    public void init() {
        FEEDMESSAGE = TestUtils.readExample();

        Producer producerMock = mock(Producer.class);

        TypedMessageBuilder typedMessageBuilderMock = mock(TypedMessageBuilder.class);
        when(typedMessageBuilderMock.key(any())).thenReturn(typedMessageBuilderMock);
        when(typedMessageBuilderMock.value(any())).thenReturn(typedMessageBuilderMock);
        when(typedMessageBuilderMock.eventTime(anyLong())).thenReturn(typedMessageBuilderMock);
        when(typedMessageBuilderMock.property(any(), any())).thenReturn(typedMessageBuilderMock);
        when(typedMessageBuilderMock.sendAsync()).thenReturn(CompletableFuture.completedFuture(null));

        when(producerMock.newMessage()).thenReturn(typedMessageBuilderMock);

        this.feedEntityPublisher = new FeedEntityPublisher(producerMock);
    }

    @Test
    public void testFeedEntityPublisherSendsFeedEntitiesToPulsar() {
        int sentTripUpdates = this.feedEntityPublisher.publishFeedMessage(FEEDMESSAGE);
        //Example file contains 35 alerts
        assertEquals(sentTripUpdates, 232);
    }
}
