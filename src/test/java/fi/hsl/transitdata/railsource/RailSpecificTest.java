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

    @Test
    public void testDelayFieldIsRemovedFromStopTimeUpdates() {
        GtfsRealtime.TripUpdate tripUpdate = RailSpecific.removeDelayFieldFromStopTimeUpdates(GtfsRealtime.TripUpdate.newBuilder()
                .setTimestamp(0)
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("trip_1"))
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate
                        .newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent
                                .newBuilder()
                                .setTime(10)
                                .setDelay(8)
                                .build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent
                                .newBuilder()
                                .setTime(15)
                                .setDelay(8)
                                .build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate
                        .newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent
                                .newBuilder()
                                .setTime(25)
                                .setDelay(6)
                                .build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent
                                .newBuilder()
                                .setTime(32)
                                .setDelay(9)
                                .build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .build());

        assertEquals(2, tripUpdate.getStopTimeUpdateCount());

        assertFalse(tripUpdate.getStopTimeUpdate(0).getArrival().hasDelay());
        assertFalse(tripUpdate.getStopTimeUpdate(0).getDeparture().hasDelay());

        assertFalse(tripUpdate.getStopTimeUpdate(1).getArrival().hasDelay());
        assertFalse(tripUpdate.getStopTimeUpdate(1).getDeparture().hasDelay());
    }
}
