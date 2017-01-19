package edu.umn.cs.kite.indexing.memory;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by amr_000 on 8/1/2016.
 */

public class MemoryHashIndexSegment implements MemoryIndexSegment<String,
        Long> {
    private String uniqueName;
    private IgniteCache<String,ArrayList<Long>> hashIndex = null;

    private int ingestedDataSize = 0;

    public MemoryHashIndexSegment(String uniqueName) {
        this(uniqueName, ConstantsAndDefaults.BATCH_UPDATE_TIME_MILLISECONDS);
    }

    public MemoryHashIndexSegment(String uniqueName, long
            batchUpdateTimeMilliseconds) {
        this.setName(uniqueName);

        //Index creation

        //Setting index configuration
        CacheConfiguration<String,ArrayList<Long>> cnfig = new
                CacheConfiguration<String,ArrayList<Long>>(this.getName());
        cnfig.setCacheMode(CacheMode.LOCAL);
        cnfig.setCacheStoreFactory(new IgniteReflectionFactory<BulkLoadCacheStore>
                (BulkLoadCacheStore.class));
        cnfig.setWriteThrough(true);

        Ignite cluster = KiteInstance.getCluster(); //getting underlying cluster
        hashIndex = cluster.getOrCreateCache(cnfig); //create index
    }

    @Override
    public void setName(String uniqueName) { this.uniqueName = uniqueName; }
    @Override
    public String getName() { return this.uniqueName; }

    public int insert(String key, Long value) {
        ArrayList<Long> listOfOne = new ArrayList<Long>();
        listOfOne.add(value);

        int inserted = insert(key,listOfOne);
        ingestedDataSize += inserted;
        return inserted;

        ////hashIndexDataStream.addData(key, (L)listOfOne);
    }

    @Override
    public int insert(String key, ArrayList<Long> values) {
        hashIndex.localLoadCache(null, hashIndex, key, values);
        ingestedDataSize += values.size();
        return values.size();
    }

    @Override
    public ArrayList<Long> search(String key, Query query) {
        ArrayList<Long> ids = hashIndex.get(key);
        return ids;
    }

    @Override
    public boolean clear() {
        hashIndex.clear();
        //GC.invoke();
        return true;
    }

    @Override
    public int getIngestedDataSize() {
        return ingestedDataSize;
    }

    public Iterator<Cache.Entry<String,ArrayList<Long>>> getEntriesIterator() {
        return hashIndex.iterator();
    }
}
