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
class HslGtfsRtPoller {
    private final String gtfsRtUrl;
    private final RailTripUpdateService railTripUpdateService;

    HslGtfsRtPoller(Config config, RailTripUpdateService railTripUpdateService) {
        this.gtfsRtUrl = config.getString("poller.gtfsrturl");
        this.railTripUpdateService = railTripUpdateService;
    }

    void poll() throws IOException {
        GtfsRealtime.FeedMessage feedMessage = readFeedMessage(gtfsRtUrl);
        handleFeedMessage(feedMessage);
    }

    static GtfsRealtime.FeedMessage readFeedMessage(String url) throws IOException {
        return readFeedMessage(new URL(url));
    }

    static GtfsRealtime.FeedMessage readFeedMessage(URL url) throws IOException {
        log.info("Reading GTFS RT feed messages from " + url);

        try (InputStream inputStream = url.openStream()) {
            return GtfsRealtime.FeedMessage.parseFrom(inputStream);
        }
    }

    private void handleFeedMessage(GtfsRealtime.FeedMessage feedMessage) {
        railTripUpdateService.sendRailTripUpdates(feedMessage);
    }

}
