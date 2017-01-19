package edu.umn.cs.kite.indexing.memory;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.indexing.disk.DiskHashIndex;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.TemporalPeriod;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by amr_000 on 8/1/2016.
 */
public class MemoryHashIndex extends MemoryIndex<String,Long> implements
        Runnable {

    public MemoryHashIndex(String uniqueName, StreamDataset stream, Attribute
                            indexAttribute, boolean loadDiskIndex){
        this(uniqueName, stream, indexAttribute, ConstantsAndDefaults
                .MEMORY_INDEX_CAPACITY, ConstantsAndDefaults.
                MEMORY_INDEX_DEFAULT_SEGMENTS, loadDiskIndex);
    }

    public MemoryHashIndex(String uniqueName, StreamDataset stream, Attribute
                           indexAttribute, int indexCapacity, int
            numIndexSegments, boolean loadDiskIndex) {
        super(uniqueName, indexCapacity, numIndexSegments, stream);
        creationTime = new Date();
        indexSegments = new ArrayList<>();
        indexSegmentsTime = new ArrayList<>();
        this.indexedAttribute = indexAttribute;

        this.correspondingDiskIndex = new DiskHashIndex(this
                .indexUniqueName+"_disk",this.stream, loadDiskIndex);
        addIndexSegment();
    }

    public int insert(String key, Long value) {
        int inserted = this.getActiveIndexSegment().insert(key,value);
        totalIngestedDataSize += inserted;
        inMemoryIngestedDataSize += inserted;
        lastInsertedMicroblogId = value;

        if(setSegmentTime) {
            Long firstMicroblogId = value;
            Microblog microblog = stream.getRecord(firstMicroblogId);
            long startTimestamp = microblog.getTimestamp();
            indexSegmentsTime.get(indexSegmentsTime.size()-1).setFrom(
                    startTimestamp);
            setSegmentTime = false;
        }
        return inserted;
    }

    @Override
    public int insert(String key, ArrayList<Long> values) {
        int inserted = this.getActiveIndexSegment().insert(key,values);
        totalIngestedDataSize += inserted;
        inMemoryIngestedDataSize += inserted;
        lastInsertedMicroblogId = values.get(values.size()-1);

        if(setSegmentTime) {
            Long firstMicroblogId = values.get(0);
            Microblog microblog = stream.getRecord(firstMicroblogId);
            long startTimestamp = microblog.getTimestamp();
            indexSegmentsTime.get(indexSegmentsTime.size()-1).setFrom(
                    startTimestamp);
            setSegmentTime = false;
        }
        return inserted;
    }

    @Override
    public int insert(List<Microblog> microblogs) {
        int inserted = 0;

        ArrayList<String> searchKeys = new ArrayList<>();

        Hashtable<String,ArrayList<Long>> temp_hash = new Hashtable<>();

        for(Microblog microblog : microblogs) {
            List<String> keys = microblog.getKeysStr(indexedAttribute);

            for(String key:keys) {
                if(key != null) {
                    inserted++;
                    ArrayList<Long> val = temp_hash.get(key);
                    if (val == null)
                        val = new ArrayList<>();
                    val.add(0, microblog.getId());
                    temp_hash.put(key, val);

                    stream.incrementIndexCount(microblog.getId());

                    /*if (DebugFlagger.flushingTestFlag == DebugFlagger
                            .TestingState.
                            TEST_ON)
                        searchKeys.add(key);*/
                }
            }
        }

        /*if(DebugFlagger.flushingTestFlag == DebugFlagger.TestingState
                .TEST_ON) {
            //print search keys
            //for(String key: searchKeys)
                //System.out.println(key);
            QueryGenerator.addSearchKeys(searchKeys);
        }*/

        for (Map.Entry<String,ArrayList<Long>> entry: temp_hash.entrySet()) {
            this.insert(entry.getKey(),entry.getValue());
        }

        postInsertion();
        return inserted;
    }

    private void postInsertion() {
        if(checkAddingIndexSegment())
            checkFlushing();
    }

    private void checkFlushing() {
        if(indexSegments.size() > this.numIndexSegments) {
            try {
                flush();
            } catch (Exception e) {
                String errMsg = e.getMessage();
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
            }
        }
    }

    @Override
    public ArrayList<Long> search(String key, Query query, TemporalPeriod
                                    searchPeriod) {
        if(query.isHash() && key != null) {
            switch (flushingPolicy.getPolicyType()) {
                case TEMPORAL_FLUSHING:
                    ArrayList<Long> answer = new ArrayList<Long>();
                    int startOverlapPeriod = KiteUtils.binarySearch_StartOverlap(
                            indexSegmentsTime, searchPeriod);

                    for (int i = startOverlapPeriod; i >= 0 && i < indexSegments
                            .size() && indexSegmentsTime.get(i).overlap
                            (searchPeriod); ++i) {

                        ArrayList<Long> answer_i = indexSegments.get(i).search(
                                key, query);

                        if(answer_i != null)
                            answer.addAll(answer_i);
                    }
                    return answer;
                case K_FLUSHING:
                    return this.getActiveIndexSegment().search(key, query);
            }
        }
        return null;
    }

    @Override
    public void flush() throws Exception {
        if(stream.isShown()) {
            String msg = "Flushing " + getName() + "...";
            System.out.println(msg);
            KiteInstance.logShow(msg);
        }
        switch (flushingPolicy.getPolicyType()) {
            case TEMPORAL_FLUSHING:

                /*if(DebugFlagger.flushingTestFlag == DebugFlagger
                    .TestingState.TEST_ON) {
                    //collect query answers of some keys from segment to be
                    // flushed
                    QueryGenerator.sampleQueriesNAnswers(100, indexSegments
                            .get(0), stream);
                }*/

                if(correspondingDiskIndex.addSegment(indexSegments.get(0),
                        indexSegmentsTime.get(0))) {
                    inMemoryIngestedDataSize -= indexSegments.get(0).
                            getIngestedDataSize();
                    indexSegments.get(0).clear();
                    indexSegments.remove(0);
                    TemporalPeriod flushedPeriod = indexSegmentsTime.remove(0);

                    /*if(DebugFlagger.flushingTestFlag == DebugFlagger.
                            TestingState.TEST_ON) {
                        QueryGenerator.flushSegment();
                        int wrongQueries = QueryGenerator.verifyQueryAnswers
                                (correspondingDiskIndex, flushedPeriod);
                        System.out.println("Got "+wrongQueries+" wrong " +
                                "queries");
                        if(wrongQueries > 0) {
                            System.out.println("Proceed enter to proceed");
                            System.console().readLine();
                        }
                    }*/
                }
                else
                    throw new Exception("Flushing Error of segment "+
                            indexSegments.get(0).getName());
                break;
            case K_FLUSHING:
                break;
        }
    }

    @Override
    protected MemoryIndexSegment getActiveIndexSegment() {
        switch (flushingPolicy.getPolicyType())
        {
            case TEMPORAL_FLUSHING:
            case K_FLUSHING:
                return indexSegments.get(indexSegments.size()-1);
        }
        return null;
    }

    @Override
    protected boolean addIndexSegment() {
        boolean addASegment = false;
        switch (flushingPolicy.getPolicyType())
        {
            case TEMPORAL_FLUSHING:
                addASegment = true;
                break;
            case K_FLUSHING:
                if(indexSegments.size() == 0)
                    addASegment = true;
                break;
        }
        if(addASegment) {
            //conclude the current segment
            long concludingTimestamp = new Date().getTime();
            if(lastInsertedMicroblogId != -1) {//only if any microblogs ingested
                Microblog lastMicroblog = stream.getRecord(
                        lastInsertedMicroblogId);
                concludingTimestamp = lastMicroblog.getTimestamp();
                indexSegmentsTime.get(indexSegmentsTime.size() - 1).setTo(
                        concludingTimestamp);
            }
            //add the new segment
            int segmentSequenceNumber = indexSegments.size();
            indexSegments.add(new MemoryHashIndexSegment
                    (getName()+"_seg"+ KiteUtils
                            .int2str(segmentSequenceNumber,4),
                            getBatchUpdateTimeMilliseconds()));
            lastSegmentationTime = new Date();
            indexSegmentsTime.add(new TemporalPeriod( new Timestamp
                    (concludingTimestamp), null));
            setSegmentTime = true;
        }
        return addASegment;
    }

    @Override
    public List<Microblog> searchDisk(int k, String key, Query query,
                                      TemporalPeriod time) {
        return correspondingDiskIndex.search(key,k,time,query);
    }

    @Override
    public void destroy() {
        for(MemoryIndexSegment segment: indexSegments)
            segment.clear();
        correspondingDiskIndex.destroy();
    }

    @Override
    //flush in a separate thread
    public void run() {
        try {
            flush();
        } catch (Exception e) {
            String errMsg = e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
        }
    }
}
