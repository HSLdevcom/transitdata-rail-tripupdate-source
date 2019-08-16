package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import lombok.extern.slf4j.Slf4j;

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
}
