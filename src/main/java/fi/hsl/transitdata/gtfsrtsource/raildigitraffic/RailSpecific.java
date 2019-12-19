package fi.hsl.transitdata.gtfsrtsource.raildigitraffic;

import com.google.transit.realtime.GtfsRealtime;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
class RailSpecific {
    static List<GtfsRealtime.FeedEntity> filterRailTripUpdates(GtfsRealtime.FeedMessage feedMessage) {
        return feedMessage.getEntityList()
                .stream()
                .filter(GtfsRealtime.FeedEntity::hasTripUpdate)
                .collect(Collectors.toList());
    }

    static GtfsRealtime.TripUpdate fixInvalidTripUpdateDelayUsage(GtfsRealtime.TripUpdate tripUpdate) {
        //If trip update has specified delay and stop time updates, timing information in stop time updates is ignored
        //-> remove delay from trip update
        if (tripUpdate.hasDelay() && tripUpdate.getStopTimeUpdateCount() != 0) {
            return tripUpdate.toBuilder().clearDelay().build();
        } else {
            return tripUpdate;
        }
    }

    static GtfsRealtime.TripUpdate removeDelayFieldFromStopTimeUpdates(GtfsRealtime.TripUpdate tripUpdate) {
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .map(GtfsRealtime.TripUpdate.StopTimeUpdate::toBuilder)
                .map(stuBuilder -> {
                    if (stuBuilder.hasArrival()) {
                        stuBuilder = stuBuilder.setArrival(removeDelayField(stuBuilder.getArrival()));
                    }
                    if (stuBuilder.hasDeparture()) {
                        stuBuilder = stuBuilder.setDeparture(removeDelayField(stuBuilder.getDeparture()));
                    }
                    return stuBuilder.build();
                })
                .collect(Collectors.toList());

        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdates).build();
    }

    private static GtfsRealtime.TripUpdate.StopTimeEvent removeDelayField(GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent) {
        if (stopTimeEvent.hasDelay() && stopTimeEvent.hasTime()) {
            return stopTimeEvent.toBuilder().clearDelay().build();
        } else {
            return stopTimeEvent;
        }
    }

    static GtfsRealtime.TripUpdate removeTripIdField(GtfsRealtime.TripUpdate tripUpdate) {
        if (tripUpdate.hasTrip() && tripUpdate.getTrip().hasTripId()) {
            return tripUpdate.toBuilder()
                    .setTrip(tripUpdate.getTrip().toBuilder().clearTripId())
                    .build();
        } else {
            return tripUpdate;
        }
    }
}
