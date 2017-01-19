package edu.umn.cs.kite.indexing.disk.spatial;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.indexing.disk.DiskIndex;
import edu.umn.cs.kite.indexing.disk.DiskIndexSegment;
import edu.umn.cs.kite.indexing.memory.MemoryIndexSegment;
import edu.umn.cs.kite.indexing.memory.spatial.MemorySpatialIndexSegment;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartitioner;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.TemporalPeriod;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by amr_000 on 11/26/2016.
 */
public class DiskSpatialIndex extends DiskIndex<GeoLocation,Microblog> {

    private SpatialPartitioner partitioner;
    private Attribute indexedAttribute;

    public DiskSpatialIndex(String indexUniqueName, StreamDataset stream,
                            Attribute indexedAttribute,
                            SpatialPartitioner partitioner, boolean load) {
        this.indexUniqueName = indexUniqueName;
        this.stream = stream;
        this.indexedAttribute = indexedAttribute;
        this.partitioner = partitioner;

        if(load)
            loadSegments();
    }

    @Override
    public boolean addSegment(MemoryIndexSegment memorySegment, TemporalPeriod
            p) {
        DiskSpatialIndexSegment segment = null;
        try {
            segment = new DiskSpatialIndexSegment (this.
                    getIndexUniqueName(), this.
                    getIndexUniqueName()+"_seg"+ KiteUtils.int2str
                    (indexSegments.size(),4),
                    this.partitioner,
                    (MemorySpatialIndexSegment) memorySegment, stream,
                    indexedAttribute);
            indexSegments.add(segment);
            indexSegmentsTime.add(p);
            appendToDisk(p);
            //TODO: check here actual segments with dir in memory
            if(indexSegments.size() > ConstantsAndDefaults
                    .NUM_DISK_SEGMENTS_IN_MEMORY_DIRECTORY) {
                int ind = indexSegments.size()-ConstantsAndDefaults
                        .NUM_DISK_SEGMENTS_IN_MEMORY_DIRECTORY-1;
                indexSegments.get(ind).discardInMemoryDirectory();
            }
        } catch (IOException e) {
            String errMsg = "Unable to flush contents of memory index " +
                    ""+memorySegment.getName()+" to HDFS.";
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return false;
        }
        return true;
    }

    @Override
    public ArrayList<Microblog> search (GeoLocation key, int k, TemporalPeriod
            period, Query query) {
        int startOverlapPeriod = KiteUtils.binarySearch_StartOverlap(
                indexSegmentsTime, period);

        if(startOverlapPeriod >= 0) {
            ArrayList<Microblog> results = new ArrayList<>();
            int answerSizeRemaining = k;
            for(int i = startOverlapPeriod; i < indexSegmentsTime.size() &&
                    indexSegmentsTime.get(i).overlap(period) && results.size
                    () < k; ++i) {
                ArrayList<Microblog> segResults = indexSegments.get(i).search
                        (key, answerSizeRemaining, query);
                results.addAll(segResults);
                answerSizeRemaining -= segResults.size();
            }
            return results;
        }
        else //no results as search temporal period is out of index time horizon
            return new ArrayList<>();
    }

    @Override
    public void destroy() {
        for(DiskIndexSegment segment: indexSegments)
            segment.destroy();
    }

    @Override
    protected DiskIndexSegment loadNextSegment() {
        DiskSpatialIndexSegment segment = null;
        try {
            segment = new DiskSpatialIndexSegment(this.
                    getIndexUniqueName(), this.
                    getIndexUniqueName()+"_seg"+KiteUtils.int2str
                    (indexSegments.size(),4), null,null, stream,
                    indexedAttribute);
            return segment;
        } catch (IOException e) {
            String errMsg = "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);return null;
        }
    }
}
