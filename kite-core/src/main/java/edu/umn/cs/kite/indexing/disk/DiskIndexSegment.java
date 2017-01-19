package edu.umn.cs.kite.indexing.disk;

import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.util.ArrayList;

/**
 * Created by amr_000 on 9/8/2016.
 */
public interface DiskIndexSegment<K> {
    ArrayList<Microblog> search(K key, int k, Query query);

    void destroy();

    boolean discardInMemoryDirectory();
}
