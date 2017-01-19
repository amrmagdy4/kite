package edu.umn.cs.kite.indexing.memory.spatial;

import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.indexing.memory.MemoryIndexSegment;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.microblogs.Microblog;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by amr_000 on 8/5/2016.
 */
public class MemorySpatialIndexSegment extends SpatialIndex implements
        MemoryIndexSegment<GeoLocation, Long>
{
    private String uniqueName;
    private int ingestedDataSize = 0;
    private StreamDataset stream;
    private Attribute indexedAttribute;
    /*
     * A map: partition id -> partition data
     */
    protected IgniteCache<Integer,SpatialPartition> spatialPartitionsData;

    public MemorySpatialIndexSegment(String uniqueName, SpatialPartitioner
            partitioner, long batchUpdateTimeMilliseconds, StreamDataset
            stream, Attribute indexedAttribute)
    {
        this.setName(uniqueName);
        this.spatialPartitioner = partitioner;
        this.stream = stream;
        this.indexedAttribute = indexedAttribute;

        //Index creation

        //Setting index configuration
        CacheConfiguration<Integer,SpatialPartition> cnfig = new
                CacheConfiguration<> (this.getName());
        cnfig.setCacheMode(CacheMode.LOCAL);
        cnfig.setCacheStoreFactory(new IgniteReflectionFactory<BulkLoadSpatialCacheStore>
                (BulkLoadSpatialCacheStore.class));
        cnfig.setWriteThrough(true);

        Ignite cluster = KiteInstance.getCluster(); //getting underlying cluster
        spatialPartitionsData = cluster.getOrCreateCache(cnfig); //create
    }

    @Override
    public void setName(String uniqueName) { this.uniqueName = uniqueName; }
    @Override
    public String getName() { return uniqueName; }

    public int insert(Integer partitionId, Long value) {
        int inserted = 0;
        inserted += spatialPartitionsData.get(partitionId).insert(value);
        ingestedDataSize += inserted;
        return inserted;
    }

    @Override
    public int insert(GeoLocation key, Long value) {
        List<Integer> spatialPartitionIds =  spatialPartitioner.overlap(key);
        int inserted = 0;
        for(Integer partitionId : spatialPartitionIds) {
            inserted += insert(partitionId, value);
        }
        return inserted;
    }

    @Override
    public int insert(GeoLocation key, ArrayList<Long> values) {
        List<Integer> spatialPartitionIds =  spatialPartitioner.overlap(key);

        int inserted = 0;
        for(Integer partitionId : spatialPartitionIds) {
            inserted += insert(partitionId, values);
        }
        return inserted;
    }

    public int insert(Integer key, ArrayList<Long> values) {
        spatialPartitionsData.localLoadCache(null,spatialPartitionsData,key,values);
        ingestedDataSize += values.size();
        return values.size();
    }

    /**
     * Get all spatial object that overlap with given location
     * If location is a spatial range, this is a traditional range query.
     * If location is a point, this gives a range query for the point's
     * spatial partition.
     * @param location: search location
     * @return list of spatial objects represents the query answer
     */
    @Override
    public ArrayList<Long> search(GeoLocation location, Query query) {
        ArrayList<Long> answer = new ArrayList<>();

        if(query.isSpatialRange()) {
            Rectangle queryRange = location instanceof Rectangle? (Rectangle)
                    location : query.getParam_Rectangle("SpatialRange");

            List<Integer> spatialPartitionIds =  spatialPartitioner.overlap
                    (queryRange);

            for(Integer partitionId : spatialPartitionIds) {
                ArrayList<Long> mids = search(partitionId);
                if(spatialPartitioner.contained(partitionId, queryRange))
                    answer.addAll(mids);
                else {
                    //filter on spatial range
                    for(Long id: mids) {
                        Microblog record = stream.getRecord(id);
                        if(record.overlap(queryRange, indexedAttribute))
                            answer.add(id);
                    }
                }
            }
        }
        else  if(query.isSpatialKNN());
            //ToDo: implement KNN
        return answer;
    }

    public ArrayList<Long> search(Integer partitionId) {
        ArrayList<Long> answer = new ArrayList<>();
        SpatialPartition sPart = spatialPartitionsData.get(partitionId);
        if(sPart != null)
            answer.addAll(sPart.getData());
        return answer;
    }

    @Override
    public boolean clear() {
        spatialPartitionsData.clear();
        //GC.invoke();
        return true;
    }

    @Override
    public int getIngestedDataSize() {
        return ingestedDataSize;
    }

    public Iterator<Cache.Entry<Integer,SpatialPartition>>
            getEntriesIterator() {
        return spatialPartitionsData.iterator();
    }
}
