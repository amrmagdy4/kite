package edu.umn.cs.kite.indexing.memory.spatial;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Created by amr_000 on 8/20/2016.
 */
public class BulkLoadSpatialCacheStore implements
        CacheStore<Integer, SpatialPartition> {
    @Override
    public void loadCache(IgniteBiInClosure<Integer, SpatialPartition>
                                 igniteBiInClosure, @Nullable Object... objects)
            throws CacheLoaderException {
        IgniteCache<Integer,SpatialPartition> cache =
                (IgniteCache<Integer,SpatialPartition>) objects[0];
        Integer key = (Integer) objects[1];
        ArrayList<Long> values = (ArrayList<Long>) objects[2];

        SpatialPartition curr_value = cache.get(key);
        if(curr_value == null)
            curr_value = new SpatialPartition(values);
        else
            curr_value.mergeData(values);
        cache.put(key, curr_value);
    }

    @Override
    public void sessionEnd(boolean b) throws CacheWriterException {

    }

    @Override
    public SpatialPartition load(Integer s)
            throws CacheLoaderException {
        return null;
    }

    @Override
    public Map<Integer, SpatialPartition> loadAll
            (Iterable<? extends Integer> iterable)
            throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Cache.Entry<? extends Integer,
            ? extends SpatialPartition> entry) throws
            CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends
            SpatialPartition>> collection) throws CacheWriterException {

    }

    @Override
    public void delete(Object o) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException
    {

    }
}
