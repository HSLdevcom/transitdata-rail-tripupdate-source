package fi.hsl.transitdata.railsource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rail source service sends tripupdates and alerts from trains
 * to corresponding tripupdate and alert topics.
 */
public class HSLRailSourceMain {

    private static final Logger log = LoggerFactory.getLogger(HSLRailSourceMain.class);

    public static void main(String[] args) {

        try {
            final Config config = ConfigParser.createConfig();
            final PulsarApplication app = PulsarApplication.newInstance(config);
            final PulsarApplicationContext context = app.getContext();
            final HslRailPoller poller = new HslRailPoller(context.getProducer(), context.getJedis(), config,
                    new RailTripUpdateService(context.getProducer()));

            final int pollIntervalInSeconds = config.getInt("poller.interval");
            final long maxTimeAfterSending = config.getDuration("poller.unhealthyAfterNotSending", TimeUnit.NANOSECONDS);
            final AtomicLong sendTime = new AtomicLong(System.nanoTime());

            if (context.getHealthServer() != null) {
                context.getHealthServer().addCheck(() -> System.nanoTime() - sendTime.get() < maxTimeAfterSending);
            }

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    poller.poll();
                    sendTime.set(System.nanoTime());
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
