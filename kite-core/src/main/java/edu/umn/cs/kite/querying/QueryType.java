package edu.umn.cs.kite.querying;

/**
 * Created by amr_000 on 12/5/2016.
 */
public enum QueryType {
    //all queries are temporal, and they may also include spatial and/or keyword
    PURE_TEMPORAL, UNINDEXED_TEMPORAL, HASH_TEMPORAL, SPATIAL_RANGE_TEMPORAL,
    SPATIAL_KNN_TEMPORAL, UNDECIDED
}
