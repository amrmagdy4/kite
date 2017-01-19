package edu.umn.cs.kite.indexing.memory.spatial;

import java.util.ArrayList;

/**
 * Created by amr_000 on 8/4/2016.
 */
public class SpatialPartition {
    //protected Rectangle partitionBoundaries;
    protected ArrayList<Long> data;

    public SpatialPartition(){data = new ArrayList<>();}
    public SpatialPartition(ArrayList<Long> data){this.data = data;}

    public boolean mergeData(SpatialPartition other) {
        this.data.addAll(other.getData());
        return true;
    }

    public boolean mergeData(ArrayList<Long> other) {
        this.data.addAll(other);
        return true;
    }

    public ArrayList<Long> getData() { return data; }

    public int insert(Long microblogId) {
        data.add(microblogId);
        return 1;
    }
}
