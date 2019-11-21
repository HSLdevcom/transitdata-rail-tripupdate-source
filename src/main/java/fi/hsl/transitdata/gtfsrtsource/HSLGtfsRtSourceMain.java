package fi.hsl.transitdata.gtfsrtsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.gtfsrtsource.raildigitraffic.RailDigitrafficFeedEntityProcessor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Gtfs Rt source polls feed messages from specified URL and publishes them individually to Pulsar
 */
public class HSLGtfsRtSourceMain {

    private static final Logger log = LoggerFactory.getLogger(HSLGtfsRtSourceMain.class);

    public static void main(String[] args) {

        try {
            final Config config = ConfigParser.createConfig();
            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();

            final FeedEntityProcessor feedEntityProcessor;
            switch (config.getString("poller.processor")) {
                case "raildigitraffic":
                    feedEntityProcessor = new RailDigitrafficFeedEntityProcessor();
                    break;
                default:
                    feedEntityProcessor = Optional::ofNullable;
                    break;
            }

            final HslGtfsRtPoller poller = new HslGtfsRtPoller(config, new FeedEntityPublisher(context.getProducer(), feedEntityProcessor));

            final int pollIntervalInSeconds = config.getInt("poller.interval");
            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    poller.poll();
                } catch (InvalidProtocolBufferException e) {
                    log.error("Cancellation message format is invalid", e);
                } catch (PulsarClientException e) {
                    log.error("Pulsar connection error", e);
                    closeApplication(app, scheduler);
                } catch (IOException e) {
                    log.error("Error with HTTP connection: " + e.getMessage(), e);
                } catch (Exception e) {
                    log.error("Unknown exception at poll cycle: ", e);
                    closeApplication(app, scheduler);
                }
            }, 0, pollIntervalInSeconds, TimeUnit.SECONDS);


        } catch (Exception e) {
            log.error("Exception at fi.hsl.transitdata.railsource.HSLRailSourceMain: " + e.getMessage(), e);
        }
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }
}
