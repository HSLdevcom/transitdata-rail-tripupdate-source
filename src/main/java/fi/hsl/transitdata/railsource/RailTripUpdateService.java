package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.TransitdataProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static fi.hsl.transitdata.railsource.RailSpecific.*;

/**
 * Sends parsed railway alerts to a Pulsar topic
 */
@Slf4j
class RailTripUpdateService {
    private Producer<byte[]> producer;

    RailTripUpdateService(Producer<byte[]> producer) {
        this.producer = producer;
    }

    int sendRailTripUpdates(GtfsRealtime.FeedMessage feedMessage) {
        int sentTripUpdates = 0;

        List<GtfsRealtime.FeedEntity> feedEntities = filterRailTripUpdates(feedMessage);
        log.info("Found {} rail trip updates", feedEntities.size());
        for (GtfsRealtime.FeedEntity feedEntity : feedEntities) {
            sendTripUpdate(fixInvalidTripUpdateDelayUsage(feedEntity.getTripUpdate()));
            sentTripUpdates++;
        }

        return sentTripUpdates;
    }

    private void sendTripUpdate(GtfsRealtime.TripUpdate tripUpdate) {
        long now = System.currentTimeMillis();

        String entityId = generateEntityId(tripUpdate);
        String tripId = tripUpdate.getTrip().getTripId();
        GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(entityId, tripUpdate, now);

        producer.newMessage()
                .key(entityId)
                .value(feedMessage.toByteArray())
                .eventTime(now)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .sendAsync()
                .thenRun(() -> log.debug("Sending TripUpdate for tripId {} with {} StopTimeUpdates and status {}",
                        tripId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));
    }

    static String generateEntityId(GtfsRealtime.TripUpdate tripUpdate) {
        return "rail_" + String.join("-", tripUpdate.getTrip().getRouteId(), tripUpdate.getTrip().getStartDate(), tripUpdate.getTrip().getStartTime(), String.valueOf(tripUpdate.getTrip().getDirectionId()));
    }
}
