package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

@Slf4j
class HslRailPoller {

    private final Producer<byte[]> producer;
    private final Jedis jedis;
    private final String railUrlString;
    private final RailTripUpdateService railTripUpdateService;

    HslRailPoller(Producer<byte[]> producer, Jedis jedis, Config config, RailTripUpdateService railTripUpdateService) {
        this.railUrlString = config.getString("poller.railurl");
        this.producer = producer;
        this.jedis = jedis;
        this.railTripUpdateService = railTripUpdateService;
    }

    void poll() throws IOException {
        GtfsRealtime.FeedMessage feedMessage = readFeedMessage(railUrlString);
        handleFeedMessage(feedMessage);
    }

    static GtfsRealtime.FeedMessage readFeedMessage(String url) throws IOException {
        return readFeedMessage(new URL(url));
    }

    static GtfsRealtime.FeedMessage readFeedMessage(URL url) throws IOException {
        log.info("Reading rail feed messages from " + url);

        try (InputStream inputStream = url.openStream()) {
            return GtfsRealtime.FeedMessage.parseFrom(inputStream);
        }
    }

    private void handleFeedMessage(GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        railTripUpdateService.sendRailTripUpdates(feedMessage);
    }

}
