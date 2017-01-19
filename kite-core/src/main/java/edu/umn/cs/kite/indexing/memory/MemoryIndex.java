package edu.umn.cs.kite.indexing.memory;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.flushing.FlushingPolicy;
import edu.umn.cs.kite.flushing.TemporalFlushing;
import edu.umn.cs.kite.indexing.disk.DiskIndex;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.TemporalPeriod;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by amr_000 on 8/1/2016.
 * This class represents a segmented index that is wholly resident in
 * main-memory. Each index segment has the actual indexing data structures.
 */

public abstract class MemoryIndex<K,V> {

    ////////////////////////////////////////////////////////////////////////////
    //Fields
    ////////////////////////////////////////////////////////////////////////////
    protected Attribute indexedAttribute;
    protected String indexUniqueName;
    protected List<MemoryIndexSegment<K,V>> indexSegments;
    protected List<TemporalPeriod> indexSegmentsTime;
    protected long batchUpdateTimeMilliseconds = ConstantsAndDefaults.
            BATCH_UPDATE_TIME_MILLISECONDS;

    protected int indexCapacity = ConstantsAndDefaults.MEMORY_INDEX_CAPACITY;
    protected int numIndexSegments = ConstantsAndDefaults
            .MEMORY_INDEX_DEFAULT_SEGMENTS;
    protected FlushingPolicy flushingPolicy = new TemporalFlushing(
            ConstantsAndDefaults.MEMORY_INDEX_CAPACITY/numIndexSegments,
            TemporalFlushing.PeriodType.DATA_SIZE);
    protected DiskIndex<K,Microblog> correspondingDiskIndex;
    protected StreamDataset stream;

    protected  Long lastInsertedMicroblogId = -1L;
    protected  boolean setSegmentTime = false;
    protected long totalIngestedDataSize = 0;
    protected int inMemoryIngestedDataSize = 0;
    protected Date creationTime = null, lastSegmentationTime = null;

    ////////////////////////////////////////////////////////////////////////////
    //Constructors and Methods
    ////////////////////////////////////////////////////////////////////////////
    public MemoryIndex(String uniqueName, StreamDataset stream) {
        this(uniqueName, ConstantsAndDefaults.MEMORY_INDEX_CAPACITY,
                ConstantsAndDefaults.MEMORY_INDEX_DEFAULT_SEGMENTS,
                stream);
    }

    public MemoryIndex(String uniqueName, int indexCapacity,
                       int numIndexSegments, StreamDataset stream) {
        this.indexUniqueName = uniqueName;
        this.batchUpdateTimeMilliseconds = batchUpdateTimeMilliseconds;
        this.stream = stream;
        this.numIndexSegments = numIndexSegments;
        this.indexCapacity = indexCapacity;
        flushingPolicy = new TemporalFlushing(indexCapacity/numIndexSegments,
                TemporalFlushing.PeriodType.DATA_SIZE);
    }

    public String getName() {
        return indexUniqueName;
    }

    public long getBatchUpdateTimeMilliseconds() {
        return batchUpdateTimeMilliseconds;
    }

    public void setIndexUniqueName(String indexUniqueName) {
        this.indexUniqueName = indexUniqueName;
    }

    public void setBatchUpdateTimeMilliseconds(long batchUpdateTimeMilliseconds) {
        this.batchUpdateTimeMilliseconds = batchUpdateTimeMilliseconds;
    }

    public void setFlushingPolicy(FlushingPolicy flushingPolicy) {
        this.flushingPolicy = flushingPolicy;
    }

    protected boolean checkAddingIndexSegment() {
        //check adding a new index segment
        boolean addSegment = false;

        if(flushingPolicy.getPolicyType()== FlushingPolicy.FlushingPolicyType
                .TEMPORAL_FLUSHING) {
            TemporalFlushing temporalFlushing = (TemporalFlushing)
                    flushingPolicy;

            if(temporalFlushing.getPeriodType() == TemporalFlushing.PeriodType.
                    TIME) {
                long currSegmentDurationMilliSeconds = creationTime.getTime() -
                        lastSegmentationTime.getTime();
                double currSegmentDurationMinutes =
                        currSegmentDurationMilliSeconds * 1.0 / (1000.0 * 60.0);
                if (currSegmentDurationMinutes >= temporalFlushing.getPeriod())
                    addSegment = true;
            } else if(temporalFlushing.getPeriodType() == TemporalFlushing.
                    PeriodType.DATA_SIZE) {
                if(getActiveIndexSegment().getIngestedDataSize() >=
                        temporalFlushing.getPeriod())
                    addSegment = true;
            }
        }
        if(addSegment) {
            addIndexSegment();

            /*if(DebugFlagger.flushingTestFlag == DebugFlagger.TestingState
                    .TEST_ON) {
                QueryGenerator.addSegment();
            }*/
        }
        return addSegment;
    }

    // Abstract methods
    public abstract int insert(K key, ArrayList<V> values);
    public abstract int insert(List<Microblog> records);
    public abstract ArrayList<V> search(K key, Query query,
                                        TemporalPeriod searchPeriod);

    public abstract void flush() throws Exception;
    protected abstract MemoryIndexSegment getActiveIndexSegment();
    protected abstract boolean addIndexSegment();

    public Attribute getIndexedAttribute() {
        return indexedAttribute;
    }

    public abstract List<Microblog> searchDisk(int k, K key, Query query,
                                               TemporalPeriod time);

    public abstract void destroy();

    public long getSize() {
        return totalIngestedDataSize;
    }

    public int getInMemorySize() {
        return inMemoryIngestedDataSize;
    }
}
