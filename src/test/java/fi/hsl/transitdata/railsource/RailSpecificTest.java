package fi.hsl.transitdata.railsource;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RailSpecificTest {
    @Test
    public void testDelayFieldIsRemovedIfTripUpdateHasStopTimeUpdates() {
        GtfsRealtime.TripUpdate tripUpdate = RailSpecific.fixInvalidTripUpdateDelayUsage(GtfsRealtime.TripUpdate.newBuilder()
            .setTimestamp(0)
            .setDelay(10)
            .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("trip_1"))
            .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate
                    .newBuilder()
                    .setStopId("1")
                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent
                            .newBuilder()
                            .setTime(10)
                            .build())
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent
                            .newBuilder()
                            .setTime(15)
                            .build())
                    .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                    .build())
            .build());

        assertEquals("trip_1", tripUpdate.getTrip().getTripId());
        assertEquals(1, tripUpdate.getStopTimeUpdateCount());

        assertFalse(tripUpdate.hasDelay());
    }
}
