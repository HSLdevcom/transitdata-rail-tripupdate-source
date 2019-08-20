package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@Slf4j
public class RailTripUpdateServiceTest {

    private static GtfsRealtime.FeedMessage FEEDMESSAGE = null;
    private static PulsarApplicationContext context;
    private RailTripUpdateService railTripUpdateService;


    @Before
    public void init() {
        FEEDMESSAGE = TestUtils.readExample();
        Producer producerMock = mock(Producer.class);
        this.railTripUpdateService = new RailTripUpdateService(producerMock);
    }

    @Test
    public void handleRailAlerts_sendValidAlert_shouldSendToProducer() throws PulsarClientException {
        Integer sentTripUpdates = this.railTripUpdateService.sendRailTripUpdates(FEEDMESSAGE);
        //Example file contains 35 alerts
        assertEquals(sentTripUpdates, 215, 0);
    }


}
