package edu.umn.cs.kite.indexing.memory;

import edu.umn.cs.kite.querying.Query;

import java.util.ArrayList;

/**
 * Created by amr_000 on 8/1/2016.
 */

public interface MemoryIndexSegment<K,V> {

    public abstract int insert(K key, V value);
    public abstract int insert(K key, ArrayList<V> value);
    public abstract ArrayList<V> search(K key, Query query);
    public abstract boolean clear();

    public void setName(String uniqueName);
    public String getName();
    public int getIngestedDataSize();
}
