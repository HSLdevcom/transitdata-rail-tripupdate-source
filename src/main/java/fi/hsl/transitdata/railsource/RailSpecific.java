package fi.hsl.transitdata.railsource;

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
            GtfsRealtime.TripUpdate fixedTripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                    .setTrip(tripUpdate.getTrip())
                    .setTimestamp(tripUpdate.getTimestamp())
                    .setVehicle(tripUpdate.getVehicle())
                    .addAllStopTimeUpdate(tripUpdate.getStopTimeUpdateList())
                    .build();

            return fixedTripUpdate;
        } else {
            return tripUpdate;
        }
    }
}
