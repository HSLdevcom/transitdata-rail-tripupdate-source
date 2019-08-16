package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.JoreDateTime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
class RailSpecific {
    static List<GtfsRealtime.TripUpdate> filterRailTripUpdates(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(GtfsRealtime.FeedEntity::hasTripUpdate)
                .map(GtfsRealtime.FeedEntity::getTripUpdate)
                .collect(Collectors.toList());
    }

    static InternalMessages.StopEstimate createRailTripUpdate(GtfsRealtime.TripUpdate tripUpdate, String serviceDayStartTime, Jedis jedis) {
        final GtfsRealtime.TripDescriptor tripDescriptor = tripUpdate.getTrip();
        InternalMessages.TripCancellation.Status status = null;

        status = getTripStatus(tripDescriptor, status);
        if (status != null) {
            if (status == InternalMessages.TripCancellation.Status.CANCELED) {

            }
            //GTFS-RT direction is mapped to 0 & 1, our cache keys are in Jore-format 1 & 2
            final int joreDirection = tripDescriptor.getDirectionId() + 1;
            final JoreDateTime startDateTime = new JoreDateTime(serviceDayStartTime, tripDescriptor.getStartDate(), tripDescriptor.getStartTime());
            final String cacheKey = TransitdataProperties.formatJoreId(
                    tripDescriptor.getRouteId(),
                    Integer.toString(joreDirection),
                    startDateTime);
            final String dvjId = jedis.get(cacheKey);

        } else {
            log.warn("TripUpdate has no schedule relationship in the trip descriptor, ignoring.");
        }
        return null;
    }

    private static InternalMessages.TripCancellation.Status getTripStatus(GtfsRealtime.TripDescriptor tripDescriptor, InternalMessages.TripCancellation.Status status) {
        if (tripDescriptor.hasScheduleRelationship()) {
            if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED) {
                status = InternalMessages.TripCancellation.Status.CANCELED;
            } else if (tripDescriptor.getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED) {
                status = InternalMessages.TripCancellation.Status.RUNNING;
            } else {
                log.warn("TripUpdate TripDescriptor ScheduledRelationship is {}", tripDescriptor.getScheduleRelationship());
            }
        }
        return status;
    }
}
