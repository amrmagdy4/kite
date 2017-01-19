package edu.umn.cs.kite.indexing.memory.spatial;

import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amr_000 on 8/4/2016.
 */
public interface SpatialPartitioner {

    //public boolean create(List<? extends GeoLocation> dataPoints,
    //                      Rectangle spaceMBR, int numPointsPerPartition);

    /**
     * Finds spatial partitions that overlap with a given geo-location
     * @param location: a given location (point, rectangle,...etc)
     * @return List of spatial partition ids that overlap with location
     */
    public List<Integer> overlap(GeoLocation location);
    public ByteStream serialize();

    boolean contained(Integer partitionId, Rectangle range);

    static SpatialPartitioner deserialize(byte [] partitionerBytes) {
        ByteStream bytes = new ByteStream(partitionerBytes);
        byte partitionerType = bytes.readByte();//0: grid partitioner
        if(partitionerType == 0) {
            Rectangle gridBoundaries = (Rectangle)GeoLocation.deserialize(bytes);
            int numRows = bytes.readInt();
            int numColumns = bytes.readInt();
            return new GridPartitioner(gridBoundaries, numRows, numColumns);
        }
        return null;
    }

    static boolean isValidType(String type) {
        ArrayList<String> spatialParitionsTypes = new ArrayList<>();
        spatialParitionsTypes.add("grid");

        return spatialParitionsTypes.contains(type);
    }
}
