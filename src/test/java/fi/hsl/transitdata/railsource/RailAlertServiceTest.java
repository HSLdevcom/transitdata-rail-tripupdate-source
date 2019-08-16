package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@Slf4j
public class RailAlertServiceTest {

    private static GtfsRealtime.FeedMessage FEEDMESSAGE = null;
    private static PulsarApplicationContext context;
    private RailTripUpdateService railTripUpdateService;


    @Before
    public void init() {
        FEEDMESSAGE = TestUtils.readExample();
        Producer producerMock = mock(Producer.class);
        Jedis jedisMock = mock(Jedis.class);
        this.railTripUpdateService = new RailTripUpdateService(producerMock, jedisMock, "04:30");
    }

    @Test
    public void handleRailAlerts_sendValidAlert_shouldSendToProducer() throws PulsarClientException {
        Integer sentTripUpdates = this.railTripUpdateService.sendRailTripUpdates(FEEDMESSAGE);
        //Example file contains 35 alerts
        assertEquals(sentTripUpdates, 35, 0);
    }


}
