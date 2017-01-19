package edu.umn.cs.kite.util;

import edu.umn.cs.kite.indexing.disk.DiskIndex;
import edu.umn.cs.kite.indexing.memory.MemoryIndexSegment;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.querying.QueryType;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by amr_000 on 9/3/2016.
 */
public class QueryGenerator {

    private static ArrayList<ArrayList<String>> searchKeys = new ArrayList<>();

    private static ArrayList<ArrayList<GeoLocation>> searchKeysGeo = new
            ArrayList<>();

    private static Hashtable<String,ArrayList<Microblog>> sampleQueryAnswers;

    private static Hashtable<GeoLocation,ArrayList<Microblog>>
            sampleQueryAnswersGeo;

    private static double latWidth = 0.01;
    private static double lngWidth = 0.001;

    public static void addSearchKeys(ArrayList<String> keys) {
        if(searchKeys.size() == 0)
            addSegment();

        searchKeys.get(searchKeys.size()-1).addAll(keys);
        while (searchKeys.get(searchKeys.size()-1).size() > 100000)
            searchKeys.get(searchKeys.size()-1).remove(0);
    }
    public static void addSegment(){
        searchKeys.add(new ArrayList<>());
        searchKeysGeo.add(new ArrayList<>());
    }

    public static Hashtable<String,ArrayList<Microblog>> sampleQueriesNAnswers(
            int numQueries, MemoryIndexSegment index, StreamDataset stream) {
        sampleQueryAnswers = new Hashtable<>();

        for(int i = 0; i < numQueries; ++i) {
            int ind = (int)Math.floor(Math.random()*searchKeys.get(0).size());
            if(ind >= searchKeys.get(0).size())
                ind = searchKeys.get(0).size()-1;
            String key = searchKeys.get(0).get(ind);
            ArrayList<Long> mids = index.search(key,new Query(QueryType.HASH_TEMPORAL));
            ArrayList<Microblog> microblogs = new ArrayList<>();
            for(int j = 0; mids != null && j < mids.size(); ++j){
                microblogs.add(stream.getRecord(mids.get(j)));
            }
            sampleQueryAnswers.put(key,microblogs);
        }
        return sampleQueryAnswers;
    }


    public static void flushSegment() {
        searchKeys.remove(0);
        searchKeysGeo.remove(0);
    }

    public static int verifyQueryAnswers(DiskIndex<String,Microblog> diskIndex,
                                         TemporalPeriod searchPeriod) {
        System.out.println("Verifying queries from disk");
        int wrongAnswers = 0;
        for(Map.Entry<String, ArrayList<Microblog>> entry : sampleQueryAnswers
        .entrySet()) {
            Date now = new Date();
            ArrayList<Microblog> memoryAnswer = entry.getValue();
            ArrayList<Microblog> diskAnswer = diskIndex.search(entry.getKey(),
                    ConstantsAndDefaults.QUERY_ANSWER_SIZE, searchPeriod,
                    new Query(QueryType.HASH_TEMPORAL));
            if(!isSimilar(memoryAnswer,diskAnswer))
                wrongAnswers++;
        }
        return wrongAnswers;
    }

    private static boolean isSimilar(ArrayList<Microblog> memoryAnswer,
                                     ArrayList<Microblog> diskAnswer) {
        if(memoryAnswer.size() != diskAnswer.size()) {
            return false;
        }
        if(isExact(memoryAnswer, diskAnswer)) //cheap similarity check
            return true;
        else {
            for (int i = 0; i < memoryAnswer.size(); ++i) {
                boolean found = false;
                for (int j = 0; j < diskAnswer.size(); ++j) {
                    if (memoryAnswer.get(i).getId() == diskAnswer.get(j).getId()) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
            return true;
        }
    }

    private static boolean isExact(ArrayList<Microblog> memoryAnswer,
                                     ArrayList<Microblog> diskAnswer) {
        if(memoryAnswer.size() != diskAnswer.size()) return false;

        for (int i = 0; i < memoryAnswer.size(); ++i) {
            if (memoryAnswer.get(i).getId() != diskAnswer.get(i).getId())
                return false;
        }
        return true;
    }

    ////////////////////////////// Geo ///////////////////////////////////
    public static void addSearchKeysGeo (ArrayList<GeoLocation> keys) {
        if (searchKeysGeo.size() == 0)
            addSegment();

        searchKeysGeo.get(searchKeysGeo.size()-1).addAll(keys);
        while (searchKeysGeo.get(searchKeysGeo.size()-1).size() > 100000)
            searchKeysGeo.get(searchKeysGeo.size()-1).remove(0);
    }

    public static Query getSpatialRangeQuery(PointLocation p) {
        Query q = new Query(QueryType.SPATIAL_RANGE_TEMPORAL);
        q.putParam_Rectangle("SpatialRange",new Rectangle(p,latWidth,lngWidth));
        return q;
    }

    public static Hashtable<GeoLocation,ArrayList<Microblog>>
    sampleQueriesNAnswersGeo(
            int numQueries, MemoryIndexSegment index, StreamDataset stream) {
        sampleQueryAnswersGeo = new Hashtable<>();

        for(int i = 0; i < numQueries; ++i) {
            int ind = (int)Math.floor(Math.random()*searchKeysGeo.get(0).size
                    ());
            if(ind >= searchKeysGeo.get(0).size())
                ind = searchKeysGeo.get(0).size()-1;
            GeoLocation key = searchKeysGeo.get(0).get(ind);
            Query q = new Query(QueryType.SPATIAL_RANGE_TEMPORAL);

            q.putParam_Rectangle("SpatialRange",new Rectangle(
                    (PointLocation)key,latWidth,lngWidth));
            ArrayList<Long> mids = index.search(key,q);
            ArrayList<Microblog> microblogs = new ArrayList<>();
            for(int j = 0; mids!=null && j < mids.size(); ++j) {
                microblogs.add(stream.getRecord(mids.get(j)));
            }
            sampleQueryAnswersGeo.put(key,microblogs);
        }
        return sampleQueryAnswersGeo;
    }

    public static int verifyQueryAnswersGeo(DiskIndex<GeoLocation,Microblog>
                                                    diskIndex,
                                         TemporalPeriod searchPeriod) {
        System.out.println("Verifying queries from disk");
        int wrongAnswers = 0;
        for(Map.Entry<GeoLocation, ArrayList<Microblog>> entry :
                sampleQueryAnswersGeo
                .entrySet()) {
            Date now = new Date();
            ArrayList<Microblog> memoryAnswer = entry.getValue();

            Query q = new Query(QueryType.SPATIAL_RANGE_TEMPORAL);
            q.putParam_Rectangle("SpatialRange",new Rectangle(
                    (PointLocation)entry.getKey(),latWidth,lngWidth));

            ArrayList<Microblog> diskAnswer = diskIndex.search(entry.getKey()
                    , ConstantsAndDefaults.QUERY_ANSWER_SIZE, searchPeriod,q);
            if(!isSimilar(memoryAnswer,diskAnswer))
                wrongAnswers++;
        }
        return wrongAnswers;
    }
}
