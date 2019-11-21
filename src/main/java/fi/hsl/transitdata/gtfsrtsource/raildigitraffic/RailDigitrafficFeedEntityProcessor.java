package fi.hsl.transitdata.gtfsrtsource.raildigitraffic;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.gtfsrtsource.FeedEntityProcessor;

import java.util.Optional;

import static fi.hsl.transitdata.gtfsrtsource.raildigitraffic.RailSpecific.*;

public class RailDigitrafficFeedEntityProcessor implements FeedEntityProcessor {
    @Override
    public Optional<GtfsRealtime.FeedEntity> process(GtfsRealtime.FeedEntity feedEntity) {
        if (feedEntity.hasTripUpdate()) {
            GtfsRealtime.TripUpdate tripUpdate = feedEntity.getTripUpdate();
            //Remove 'delay' field from trip update as stop time updates should be used to provide timing information
            tripUpdate = fixInvalidTripUpdateDelayUsage(tripUpdate);
            //Remove 'delay' field from stop time updates as raildigitraffic2gtfsrt API only provides inaccurate values
            tripUpdate = removeDelayFieldFromStopTimeUpdates(tripUpdate);
            //Remove 'trip_id' field from trip descriptor as we don't know if trip id provided by raildigitraffic2gtfsrt API is the same as in static GTFS feed used by Google and others
            tripUpdate = removeTripIdField(tripUpdate);

            return Optional.of(feedEntity.toBuilder().setId(generateEntityId(tripUpdate)).setTripUpdate(tripUpdate).build());
        } else {
            return Optional.empty();
        }
    }

    private static String generateEntityId(GtfsRealtime.TripUpdate tripUpdate) {
        return "rail_" + String.join("-", tripUpdate.getTrip().getRouteId(), tripUpdate.getTrip().getStartDate(), tripUpdate.getTrip().getStartTime(), String.valueOf(tripUpdate.getTrip().getDirectionId()));
    }
}
