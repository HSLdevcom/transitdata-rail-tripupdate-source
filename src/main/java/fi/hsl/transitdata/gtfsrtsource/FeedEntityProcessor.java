package fi.hsl.transitdata.gtfsrtsource;

import com.google.transit.realtime.GtfsRealtime;

import java.util.Optional;

/**
 * Interface to process feed entities, used for e.g. transforming entity ids
 */
public interface FeedEntityProcessor {
    /**
     * Processes feed entity
     * @param feedEntity Feed entity
     * @return Optional feed entity. Empty if feed entity should not be published.
     */
    Optional<GtfsRealtime.FeedEntity> process(GtfsRealtime.FeedEntity feedEntity);
}
