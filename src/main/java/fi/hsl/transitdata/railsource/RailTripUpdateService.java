package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.JoreDateTime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static fi.hsl.transitdata.railsource.RailSpecific.filterRailTripUpdates;

/**
 * Sends parsed railway alerts to a Pulsar topic
 */
@Slf4j
class RailTripUpdateService {
    private final Jedis jedis;
    private final String serviceDayStartTime;
    private Producer<byte[]> producer;

    RailTripUpdateService(Producer<byte[]> producer, Jedis jedis, String serviceDayStartTime) {
        this.producer = producer;
        this.jedis = jedis;
        this.serviceDayStartTime = serviceDayStartTime;
    }

    Integer sendRailTripUpdates(GtfsRealtime.FeedMessage feedMessage) throws PulsarClientException {
        AtomicReference<Integer> sentCancellations = new AtomicReference<>(0);
        List<GtfsRealtime.TripUpdate> tripUpdates = filterRailTripUpdates(feedMessage);
        log.info("Found {} rail alerts", tripUpdates.size());
        final long timestampMs = feedMessage.getHeader().getTimestamp() * 1000;
        for (GtfsRealtime.TripUpdate tripUpdate : tripUpdates) {
            sendCancellations(tripUpdate, timestampMs, sentCancellations);
        }
        return sentCancellations.get();
    }

    private void sendCancellations(GtfsRealtime.TripUpdate tripUpdate, long timestampMs, AtomicReference<Integer> sentCancellations) throws PulsarClientException {
        try {
            final GtfsRealtime.TripDescriptor tripDescriptor = tripUpdate.getTrip();

            InternalMessages.TripCancellation.Status status = null;

            if (tripDescriptor.hasScheduleRelationship()) {
                if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
                    status = InternalMessages.TripCancellation.Status.CANCELED;
                } else if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED) {
                    status = InternalMessages.TripCancellation.Status.RUNNING;
                } else {
                    log.warn("TripUpdate TripDescriptor ScheduledRelationship is {}", tripDescriptor.getScheduleRelationship());
                }
            }

            if (status != null) {
                if (status == InternalMessages.TripCancellation.Status.CANCELED) {
                    //GTFS-RT direction is mapped to 0 & 1, our cache keys are in Jore-format 1 & 2
                    final int joreDirection = tripDescriptor.getDirectionId() + 1;
                    final JoreDateTime startDateTime = new JoreDateTime(serviceDayStartTime, tripDescriptor.getStartDate(), tripDescriptor.getStartTime());

                    final String cacheKey = TransitdataProperties.formatJoreId(
                            tripDescriptor.getRouteId(),
                            Integer.toString(joreDirection),
                            startDateTime);
                    final String dvjId = jedis.get(cacheKey);
                    if (dvjId != null) {
                        InternalMessages.TripCancellation tripCancellation = createTripCancellationPayload(tripDescriptor, joreDirection, dvjId, status, Optional.of(startDateTime));
                        producer.newMessage().value(tripCancellation.toByteArray())
                                .eventTime(timestampMs)
                                .key(dvjId)
                                .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                                .send();
                        log.info("Produced a cancellation for trip: " + tripCancellation.getRouteId() + "/" +
                                tripCancellation.getDirectionId() + "-" + tripCancellation.getStartTime() + "-" +
                                tripCancellation.getStartDate());

                    } else {
                        log.error("Failed to produce trip cancellation message, could not find dvjId from Redis for key " + cacheKey);
                    }
                } else if (status == InternalMessages.TripCancellation.Status.RUNNING) {
                    //GTFS-RT direction is mapped to 0 & 1, our cache keys are in Jore-format 1 & 2
                    final int joreDirection = tripDescriptor.getDirectionId() + 1;
                    final JoreDateTime startDateTime = new JoreDateTime(serviceDayStartTime, tripDescriptor.getStartDate(), tripDescriptor.getStartTime());

                    final String cacheKey = TransitdataProperties.formatJoreId(
                            tripDescriptor.getRouteId(),
                            Integer.toString(joreDirection),
                            startDateTime);
                    final String dvjId = jedis.get(cacheKey);
                    if (dvjId != null) {
                        InternalMessages.StopEstimate stopEstimate = createStopEstimatePayload(tripDescriptor, dvjId, null, 1L, 1L, null);

                        producer.newMessage().value(stopEstimate.toByteArray())
                                .eventTime(timestampMs)
                                .key(dvjId)
                                .property(TransitdataProperties.KEY_DVJ_ID, dvjId)
                                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation.toString())
                                .send();


                    }

                } else {
                    log.warn("TripUpdate has no schedule relationship in the trip descriptor, ignoring.");
                }
            }
        } catch (PulsarClientException pe) {
            log.error("Failed to send message to Pulsar", pe);
            throw pe;
        } catch (JedisConnectionException e) {
            log.error("Failed to connect to Redis", e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle cancellation message", e);
        }

    }

    static InternalMessages.TripCancellation createTripCancellationPayload(final GtfsRealtime.TripDescriptor tripDescriptor,
                                                                           int joreDirection, String dvjId,
                                                                           InternalMessages.TripCancellation.Status status, Optional<JoreDateTime> startDateTime) {
        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder()
                .setTripId(dvjId)
                .setRouteId(tripDescriptor.getRouteId())
                .setDirectionId(joreDirection)
                .setStartDate(tripDescriptor.getStartDate())
                .setStartTime(tripDescriptor.getStartTime())
                .setStatus(status);
        startDateTime.ifPresent(dateTime -> {
            builder.setStartDate(dateTime.getJoreDateString());
            builder.setStartTime(dateTime.getJoreTimeString());
        });
        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.setSchemaVersion(builder.getSchemaVersion());

        return builder.build();
    }

    static InternalMessages.StopEstimate createStopEstimatePayload(final GtfsRealtime.TripDescriptor tripDescriptor, String dvjId,
                                                                   InternalMessages.StopEstimate.Status status, long estimatedTimeUtcMs, long lastModifiedTimeUtcMs, InternalMessages.TripInfo tripInfo) {
        InternalMessages.StopEstimate.Builder builder = InternalMessages.StopEstimate.newBuilder()
                .setStopId(dvjId)
                .setEstimatedTimeUtcMs(estimatedTimeUtcMs)
                .setLastModifiedUtcMs(lastModifiedTimeUtcMs)
                .setTripInfo(tripInfo)
                .setStatus(status);
        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.setSchemaVersion(builder.getSchemaVersion());

        return builder.build();
    }
}
