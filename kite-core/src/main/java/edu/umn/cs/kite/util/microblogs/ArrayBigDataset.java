package edu.umn.cs.kite.util.microblogs;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.querying.condition.Condition;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.TemporalPeriod;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by amr_000 on 9/14/2016.
 */
public class ArrayBigDataset implements Runnable {

    private int chunkCapacity;
    private int maxCapacity;
    private ArrayList<Microblog[]> data = new ArrayList<>();
    private ArrayList<Long> dataFirsts = new ArrayList<>();
    private ArrayList<TemporalPeriod> dataTimes = new ArrayList<>();
    private ArrayList<Integer[]> indexesCount = new ArrayList<>();
    private ArrayList<Object> dataLocks = new ArrayList<>();

    private Microblog[] currChunk = null;
    private Integer[] currIndexesCount = null;
    private Long currChunkFirstId = null;
    private Long currChunkFirstTimestamp = null;
    private Long currChunkLastTimestamp = null;
    private Object currChunkLock = new Object();

    private int dataInd;
    private int size;

    private boolean addChunkInProgress = false;
    private boolean discardInProgress = false;

    public ArrayBigDataset() {
        this(ConstantsAndDefaults.MAX_MEMORY_CHUNK_CAPACITY);
    }
    public ArrayBigDataset(int capacity) {
        this.maxCapacity = capacity;
        this.chunkCapacity = capacity > ConstantsAndDefaults.
                MAX_MEMORY_CHUNK_CAPACITY? ConstantsAndDefaults.
                MAX_MEMORY_CHUNK_CAPACITY : capacity;

        currChunk = new Microblog[chunkCapacity];
        currIndexesCount = new Integer[chunkCapacity];

        dataInd = 0;
        size = 0;
    }

    private void addChunck() {
        addChunkInProgress = true;
        while(discardInProgress);

        //conclude existing chunk
        data.add(currChunk);
        dataLocks.add(new Object());
        dataFirsts.add(currChunkFirstId);
        dataTimes.add(new TemporalPeriod(currChunkFirstTimestamp,
                currChunkLastTimestamp));
        indexesCount.add(currIndexesCount);
        size += dataInd;

        //add a new chunk
        synchronized (currChunkLock) {
            currChunk = new Microblog[chunkCapacity];
            currIndexesCount = new Integer[chunkCapacity];
            dataInd = 0;
            currChunkFirstId = null;
            currChunkFirstTimestamp = null;
        }
        addChunkInProgress = false;
    }

    private void add(Microblog record) {
        if(dataInd >= chunkCapacity)
            addChunck();

        currChunk[dataInd] = record;
        currChunkLastTimestamp = record.getTimestamp();
        if(dataInd == 0) {
            currChunkFirstId = currChunk[0].getId();
            currChunkFirstTimestamp = currChunk[0].getTimestamp();
        }
        dataInd++;
    }

    public void add(List<Microblog> records) {
        if((dataInd+records.size()) > chunkCapacity) {
            //will need to conclude the current chunk
            for(Microblog record:records)
                add(record);
        }
        else { //all data fits in the current chunk
            Iterator<Microblog> itr = records.iterator();
            if(dataInd == 0 && records.size() > 0) {
                //insert first microblog
                Microblog firstMicroblog = itr.next();
                add(firstMicroblog);
            }
            while (itr.hasNext()) {
                currChunk[dataInd] = itr.next();
                dataInd++;
            }
            currChunkLastTimestamp = currChunk[dataInd-1].getTimestamp();
        }
    }

    public int size() {
        return size+dataInd;
    }

    public void checkFlushing() {
        //check for full memory
        if(this.isFull()) {
            //invoke streamData flushing (discard method)
            Thread discardthread = new Thread(this);
            discardthread.start();
            //wait 50 milliseconds to empty some memory
            KiteUtils.busyWait(50);
        }
    }
    public Microblog getRecord(Long id) {
        Pair<Integer,Integer> locator = locateRecord(id);
        if(locator == null)
            return null;
        int chunkId = locator.getKey();
        int chunckInd = locator.getValue();
        if(chunkId < 0)
            return currChunk[chunckInd];
        else
            return data.get(chunkId)[chunckInd];
    }

    public void incrementIndexCount(long id) {
        Pair<Integer,Integer> locator = locateRecord(id);
        int chunkId = locator.getKey();
        int chunckInd = locator.getValue();
        if(chunkId < 0) {
            if(currIndexesCount[chunckInd] == null)
                currIndexesCount[chunckInd] = 1;
            else
                currIndexesCount[chunckInd]++;
        }
        else {
            if(indexesCount.get(chunkId)[chunckInd] == null)
                indexesCount.get(chunkId)[chunckInd] = 1;
            else
                indexesCount.get(chunkId)[chunckInd]++;
        }
    }

    public void decrementIndexCount(long id) {
        Pair<Integer,Integer> locator = locateRecord(id);
        int chunkId = locator.getKey();
        int chunckInd = locator.getValue();
        if(chunkId < 0) {
            if(currIndexesCount[chunckInd] != null)
                currIndexesCount[chunckInd]--;
        }
        else {
            if(indexesCount.get(chunkId)[chunckInd] != null)
                indexesCount.get(chunkId)[chunckInd]--;
        }
    }

    private Pair<Integer,Integer> locateRecord(long id) {
        while (addChunkInProgress);

        if(dataInd > 0 && id >= currChunkFirstId) {
            return new Pair<>(-1,(int)(id- currChunkFirstId));
        }
        else {
            int i = dataFirsts.size()-1;
            long currFirst = dataFirsts.get(i);

            while (i >= 0 && (currFirst = dataFirsts.get(i)) > id)
                --i;

            if(i < 0) {
                return null;
            }
            else
                return new Pair<>(i,(int)(id-dataFirsts.get(i)));
        }
    }

    private Pair<Integer,Integer> locateLatest(TemporalPeriod time) {
        //locate latest record that lies within the "time"
        //return value is <chunk index, record index>
        //<-1,x> means currChunk,
        // <-1,-1> means "time" older than all memory data

        while (addChunkInProgress);

        //locate chunck where the latest timestamp exists
        if(currChunkFirstTimestamp < time.to().getTime()) {
            //search currChunk
            int recordInd = KiteUtils.binarySearch(currChunk, 0, dataInd - 1,
                    time.to());
            return new Pair<>(-1, recordInd);
        }

        int endOverlapPeriod = KiteUtils.binarySearch_EndOverlap(dataTimes,
                time);

        if(endOverlapPeriod >= 0) {
            //determine latest recod index
            int recordInd = KiteUtils.binarySearch(data.get(endOverlapPeriod)
                    , 0, data.get(endOverlapPeriod).length-1, time.to());
            return new Pair<>(endOverlapPeriod, recordInd);
        } else {
            //search old data in disk
            return new Pair<>(-1, -1);
        }
    }

    public boolean isFull() {
        return this.size() >= maxCapacity;
    }

    @Override
    public void run() {
        try {
            this.discard();
        }  catch (Exception e) {
            String errMsg = "Accidental error!!";
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
        }
    }

    public int discard() {
        int totalDiscarded = 0;
        if(!addChunkInProgress) {
            discardInProgress = true;
            boolean useCurrChunk = false;
            if (data.size() > 0) {
                for (int chunk = 0; chunk < data.size() && data.get(chunk) !=
                        null; ++chunk) {
                    synchronized (dataLocks.get(chunk)) {
                        int numNulls = 0;
                        for (int i = 0; i < data.get(chunk).length; ++i) {
                            if (data.get(chunk)[i] == null)
                                ++numNulls;
                            else if ((indexesCount.get(chunk)[i] == null ||
                                    indexesCount.get(chunk)[i] <= 0)) {
                                data.get(chunk)[i] = null;
                                ++numNulls;
                                --size;
                                ++totalDiscarded;
                                if (addChunkInProgress)
                                    break;
                            }
                        }
                        if(numNulls == data.get(chunk).length) {
                            //remove this chunk
                            data.set(chunk,null);
                        }
                    }
                    if(addChunkInProgress)
                        break;
                    if (totalDiscarded >= ConstantsAndDefaults
                            .MEMORY_DATA_FLUSH_THRESHOLD * this.maxCapacity)
                        break;
                }
                if (!addChunkInProgress && totalDiscarded < ConstantsAndDefaults
                        .MEMORY_DATA_FLUSH_THRESHOLD * this.size())
                    useCurrChunk = true;
            } else
                useCurrChunk = true;

            if (useCurrChunk && !addChunkInProgress) {
                synchronized (currChunkLock) {
                    int currChunkLength = dataInd;
                    for (int i = 0; i < currChunkLength; ++i) {
                        if (currChunk[i] != null && (currIndexesCount[i] == null
                                || currIndexesCount[i] <= 0)) {
                            currChunk[i] = null;
                            --size;
                            ++totalDiscarded;
                            if (addChunkInProgress)
                                break;
                        }
                    }
                }
            }
            discardInProgress = false;
        }
        //GC.invoke();
        return totalDiscarded;
    }

    public ArrayList<Microblog> getLatestK(int k, TemporalPeriod time,
                                           Condition condition) {
        //get latest "k" microblogs within the "time" period
        Pair<Integer, Integer> locator = locateLatest(time);
        int chunkInd = locator.getKey();
        int recordInd = locator.getValue();

        ArrayList<Microblog> latestK = new ArrayList<>(k);

        if(chunkInd < 0 && recordInd < 0) //search disk data, return empty set
            return latestK;

        Microblog [] currChnk;
        int currChnkInd = chunkInd;
        int currRecordInd = recordInd;

        int counter = 0;
        boolean first = true;
        do {
            if(currChnkInd >= 0) {
                currChnk = data.get(currChnkInd);
                if(!first)
                    currRecordInd = currChnk.length-1;
            } else {
                currChnk = currChunk;
                if(!first)
                    currRecordInd = dataInd-1;
            }

            first = false;
            for(int i = currRecordInd; i >= 0 && counter < k; --i) {
                if(currChnk[i] != null) {
                    if(condition == null || (condition != null && currChnk[i]
                            .match(condition))) {
                        latestK.add(currChnk[i]);
                        ++counter;
                    }
                }
            }
            --currChnkInd;
        } while (counter < k && currChnkInd <= -1);

        if(latestK.size() < k)
            latestK.clear();//search disk for complete answer

        return latestK;
    }

    public Microblog getLast() {
        //System.out.println("dataInd="+dataInd);
        int skipped = 0;
        int lastInd = dataInd;
        while(lastInd > 0) {
            if (currChunk[lastInd - 1] != null) {
                //System.out.println("skipped="+skipped);
                return currChunk[lastInd - 1];
            }
            --lastInd;
            skipped++;
        }
        //System.out.println("skipped whole currChunk with skipped="+skipped);
        for(int chunk = data.size()-1; chunk >= 0; --chunk){
            for(int i = data.get(chunk).length-1; i >= 0; --i) {
                if (data.get(chunk)[i] != null) {
                    //System.out.println("skipped="+skipped);
                    return data.get(chunk)[i];
                }
                skipped++;
            }
        }
        //System.out.println("final skipped="+skipped);
        return null;
    }
}
