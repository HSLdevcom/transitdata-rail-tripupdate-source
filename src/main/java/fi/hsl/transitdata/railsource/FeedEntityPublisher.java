package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.TransitdataProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Optional;

/**
 * Sends feed entities individually from feed message to Pulsar topic
 */
@Slf4j
public class FeedEntityPublisher {
    private Producer<byte[]> producer;
    private FeedEntityProcessor feedEntityProcessor;

    public FeedEntityPublisher(Producer<byte[]> producer) {
        this(producer, Optional::ofNullable);
    }

    public FeedEntityPublisher(Producer<byte[]> producer, FeedEntityProcessor feedEntityProcessor) {
        this.producer = producer;
        this.feedEntityProcessor = feedEntityProcessor;
    }

    public int publishFeedMessage(GtfsRealtime.FeedMessage feedMessage) {
        log.info("Found {} feed entities", feedMessage.getEntityList().size());
        int published = feedMessage.getEntityList().stream().map(feedEntityProcessor::process).mapToInt(feedEntity -> {
            if (feedEntity.isPresent()) {
                return sendFeedEntity(feedEntity.get()) ? 1 : 0;
            } else {
                return 0;
            }
        }).sum();
        return published;
    }

    private boolean sendFeedEntity(GtfsRealtime.FeedEntity feedEntity) {
        long now = System.currentTimeMillis();

        String entityId = feedEntity.getId();

        GtfsRealtime.FeedMessage feedMessage;
        if (feedEntity.hasTripUpdate()) {
            feedMessage = FeedMessageFactory.createDifferentialFeedMessage(entityId, feedEntity.getTripUpdate(), now);
        } else if (feedEntity.hasVehicle()) {
            feedMessage = FeedMessageFactory.createDifferentialFeedMessage(entityId, feedEntity.getVehicle(), now);
        } else {
            log.warn("Unsupported feed entity (id: {}, has trip update: {}, has vehicle: {}, has alert: {})", entityId, feedEntity.hasTripUpdate(), feedEntity.hasVehicle(), feedEntity.hasAlert());
            return false;
        }

        producer.newMessage()
                .key(entityId)
                .value(feedMessage.toByteArray())
                .eventTime(now)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .sendAsync()
                .whenComplete((messageId, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof PulsarClientException) {
                            log.error("Failed to send message to Pulsar", throwable);
                        } else {
                            log.error("Unexpected error", throwable);
                        }
                    }

                    if (messageId != null) {
                        if (feedMessage.getEntity(0).hasTripUpdate()) {
                            GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntity(0).getTripUpdate();
                            log.debug("Sending TripUpdate for entity {} with {} StopTimeUpdates and status {}",
                                    entityId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship());
                        } else if (feedMessage.getEntity(0).hasVehicle()) {
                            GtfsRealtime.VehiclePosition vehiclePosition = feedMessage.getEntity(0).getVehicle();
                            log.debug("Sending VehiclePosition for entity {}", entityId);
                        }
                    }
                });
        return true;
    }
}
