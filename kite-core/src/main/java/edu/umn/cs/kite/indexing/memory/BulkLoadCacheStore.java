package edu.umn.cs.kite.indexing.memory;

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
public class BulkLoadCacheStore implements
        CacheStore<String, ArrayList<Long>> {
    @Override
    public void loadCache(IgniteBiInClosure<String, ArrayList<Long>>
        igniteBiInClosure, @Nullable Object... objects) throws CacheLoaderException {
        IgniteCache<String,ArrayList<Long>> cache =
                (IgniteCache<String,ArrayList<Long>>) objects[0];
        String key = (String) objects[1];
        ArrayList<Long> values = (ArrayList<Long>) objects[2];
        ArrayList<Long> curr_value = cache.get(key);
        if(curr_value == null)
            curr_value = new ArrayList<Long>();
        curr_value.addAll(values);
        cache.put(key, curr_value);
    }

    @Override
    public void sessionEnd(boolean b) throws CacheWriterException {
    }

    @Override
    public ArrayList<Long> load(String s) throws CacheLoaderException {
        return null;
    }

    @Override
    public Map<String, ArrayList<Long>> loadAll(Iterable<? extends String>
                                                         iterable)
            throws CacheLoaderException {
        return null;
    }

    @Override
    public void write(Cache.Entry<? extends String, ? extends ArrayList<Long>>
                                  entry) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends String, ? extends
            ArrayList<Long>>> collection) throws CacheWriterException {

    }

    @Override
    public void delete(Object o) throws CacheWriterException {

    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException
    {

    }
}
