package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static fi.hsl.transitdata.railsource.RailSpecific.filterRailTripUpdates;

/**
 * Sends parsed railway alerts to a Pulsar topic
 */
@Slf4j
class RailTripUpdateService {
    private final String serviceDayStartTime;
    private Producer<byte[]> producer;

    RailTripUpdateService(Producer<byte[]> producer, String serviceDayStartTime) {
        this.producer = producer;
        this.serviceDayStartTime = serviceDayStartTime;
    }

    Integer sendRailTripUpdates(GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        AtomicReference<Integer> sentTripUpdates = new AtomicReference<>(0);
        List<GtfsRealtime.TripUpdate> tripUpdates = filterRailTripUpdates(feedMessage);
        log.info("Found {} rail alerts", tripUpdates.size());
        log.info("Example tripupdate sent: {}", tripUpdates.get(0));
        for (GtfsRealtime.TripUpdate tripUpdate : tripUpdates) {
        sendTripUpdates(tripUpdate, sentTripUpdates);
        }
        return sentTripUpdates.get();
    }

    private void sendTripUpdates(GtfsRealtime.TripUpdate tripUpdate, AtomicReference<Integer> sentTripUpdates) throws PulsarClientException {
        sendPulsarPayloads(tripUpdate);
        sentTripUpdates.getAndSet(sentTripUpdates.get() + 1);
    }


    private void sendPulsarPayloads(GtfsRealtime.TripUpdate tripUdpate) throws PulsarClientException {
        try {
            producer.newMessage().value(tripUdpate.toByteArray())
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                    .send();
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (Exception e) {
            log.error("Failed to handle alert message", e);
        }
    }
}
