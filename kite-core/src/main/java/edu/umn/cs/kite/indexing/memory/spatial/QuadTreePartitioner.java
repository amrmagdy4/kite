package edu.umn.cs.kite.indexing.memory.spatial;

import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.List;

/**
 * Created by amr_000 on 8/6/2016.
 */
public class QuadTreePartitioner implements SpatialPartitioner {
    @Override
    public List<Integer> overlap(GeoLocation location) {
        return null;
    }

    @Override
    public ByteStream serialize() {
        return null;
    }

    @Override
    public boolean contained(Integer partitionId, Rectangle range) {
        return false;
    }
}
