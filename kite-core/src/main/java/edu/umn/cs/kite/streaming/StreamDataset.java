package edu.umn.cs.kite.streaming;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.indexing.memory.MemoryHashIndex;
import edu.umn.cs.kite.indexing.memory.MemoryIndex;
import edu.umn.cs.kite.indexing.memory.spatial.MemorySpatialIndex;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartitioner;
import edu.umn.cs.kite.preprocessing.Preprocessor;
import edu.umn.cs.kite.querying.MQLResults;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.querying.QueryType;
import edu.umn.cs.kite.querying.condition.Condition;
import edu.umn.cs.kite.util.*;
import edu.umn.cs.kite.util.microblogs.ArrayBigDataset;
import edu.umn.cs.kite.util.microblogs.Microblog;
import edu.umn.cs.kite.util.serialization.ByteStream;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


/**
 * Created by amr_000 on 9/2/2016.
 */
public class StreamDataset implements Runnable {

    private String uniqueName;
    private ArrayBigDataset streamData;
    private IdGenerator currId = null;

    private StreamingDataSource stream;
    private ArrayList<MemoryIndex> indexes;
    private ArrayList<Boolean> indexesActivators;
    private Preprocessor<String, Microblog> preprocessor;

    //recovery data structures
    private ByteStream recoveryData = new ByteStream(ConstantsAndDefaults
            .RECOVERY_BLK_SIZE_BYTES);
    private List<List<Microblog>> tempRecoveryDataList;
    boolean appendingToRecoveryData = false;
    private int recoveryBlkCounter = 0;
    private Object recoveryDataLock = new Object();
    private double microblogLengthEstimate = 200;//initiailly 200 bytes
    private int microblogLengthEstimateCount = 0;

    //Aux flags and counters
    private boolean starting = true;
    private boolean showEnable = false;
    private boolean stopped = false;
    private boolean resuming = false;
    int indexInInsertion = -1;

    long dataInserted = 0;
    int streamBatchesCount = 0;
    private boolean insertionInProgress = false;

    public Microblog getRecord(Long id) {
        Microblog record = streamData.getRecord(id);
        if(record != null)
            return record;
        else {
            //retrieve from disk
            record = getRecordFromDisk(id); //for now
            return record;
        }
    }

    private Microblog getLast() {
        return streamData.getLast();
    }

    private Microblog getRecordFromDisk(Long id) {
        //search the disk buffer before accessing the disk
        byte [] diskBufferData = null;
        synchronized (recoveryDataLock) {
            diskBufferData = recoveryData.getBytesClone();
        }
        if(diskBufferData.length > 0) {
            Pair<Pair<Boolean,Microblog>,Pair<Long,Long>> searchResults =
                    getLatestKFromBytes(id, diskBufferData);
            boolean searchSucceed = searchResults.getKey().getKey();
            if(searchSucceed)
                return searchResults.getKey().getValue();
        }

        //search disk
        Pair<String,String> dirFileidPair = getRecoveryFileIdPrefix();
        String parentDir = dirFileidPair.getKey();
        String fileIdPrefix = dirFileidPair.getValue();

        long baseId = 0;
        int low = 0;
        int high = recoveryBlkCounter-1;


        while (low <= high) {
            int estimateBlockId = low +(int)((id-baseId)*microblogLengthEstimate
                    /ConstantsAndDefaults.RECOVERY_BLK_SIZE_BYTES);

            if(estimateBlockId < low)
                estimateBlockId = low;
            if(estimateBlockId > high)
                estimateBlockId = high;

            String fileId = fileIdPrefix+KiteUtils.getRecoveryFileId
                    (estimateBlockId);
            Pair<Pair<Boolean,Microblog>,Pair<Long,Long>> searchResults =
                    searchHDFSBlock(id, parentDir, fileId);
            boolean searchSucceed = searchResults.getKey().getKey();
            if(searchSucceed)
                return searchResults.getKey().getValue();
            else {
                Long firstId = searchResults.getValue().getKey();
                Long lastId = searchResults.getValue().getValue();

                int length = (int)(lastId-firstId);
                double recordLengthEstimate = ConstantsAndDefaults
                        .RECOVERY_BLK_SIZE_BYTES/length;

                microblogLengthEstimate =
                        (microblogLengthEstimate*microblogLengthEstimateCount+
                                recordLengthEstimate)/
                                (microblogLengthEstimateCount+1);
                microblogLengthEstimateCount++;
                if(id < firstId)
                    high = estimateBlockId-1;
                else if(id > lastId) {
                    low = estimateBlockId + 1;
                    baseId = lastId+1;
                }
            }
        }
        return null;
    }

    private List<Microblog> getLatestKFromDisk(int k, TemporalPeriod time,
                                               Condition condition) {
        //search the disk for latest "k" microblogs within "time"
        List<Microblog> latestK = null;
        boolean continuingSearch = false;

        //search the disk buffer before accessing the disk
        byte [] diskBufferData = null;
        long blockTimeWidth = 5000;
        long baseTimestamp = new Date().getTime()-blockTimeWidth;
        int blockTimeWidthCount = 0;

        final long latestTimestamp = time.to().getTime();
        synchronized (recoveryDataLock) {
            diskBufferData = recoveryData.getBytesClone();
        }
        if(diskBufferData.length > 0) {
            Pair<Pair<Boolean,List<Microblog>>,Pair<Long,Long>> searchResults =
                    getLatestKFromBytes(k, time, diskBufferData);
            boolean searchSucceed = searchResults.getKey().getKey();
            if(searchSucceed) {
                if(condition == null)
                    latestK = searchResults.getKey().getValue();
                else {
                    latestK = new ArrayList<>();
                    List<Microblog> tmpList = searchResults.getKey().getValue();
                    for(Microblog record: tmpList) {
                        if(record.match(condition))
                            latestK.add(record);
                    }
                }
                if(latestK.size() < k)
                    continuingSearch = true;
                else
                    return latestK;
            } else {
                long firstTimestamp = searchResults.getValue().getKey();
                long lastTimestamp = searchResults.getValue().getValue();
                blockTimeWidth = lastTimestamp-firstTimestamp;
                baseTimestamp = firstTimestamp;
            }
        }

        //search disk
        Pair<String,String> dirFileidPair = getRecoveryFileIdPrefix();
        String parentDir = dirFileidPair.getKey();
        String fileIdPrefix = dirFileidPair.getValue();

        if(continuingSearch) {
            int blockId = recoveryBlkCounter-1;
            if(blockId >= 0) {
                int restCount = k - latestK.size();
                String fileId = fileIdPrefix + KiteUtils.getRecoveryFileId
                        (blockId);

                List<Microblog> restK = getLatestKFromHDFSBlock(restCount, null,
                        parentDir, fileId);
                if(condition == null)
                    latestK.addAll(restK);
                else {
                    for(Microblog microblog: restK)
                        if(microblog.match(condition))
                            latestK.add(microblog);
                }
            }
            return latestK;
        } else {
            //long baseId = 0;
            int low = 0;
            int high = recoveryBlkCounter-1;

            while (low <= high) {
                int estimateBlockId = high - (int)
                        (Math.ceil(Math.abs(latestTimestamp-baseTimestamp)/
                                (1.0*blockTimeWidth)));

                if(estimateBlockId < low)
                    estimateBlockId = low;
                if(estimateBlockId > high)
                    estimateBlockId = high;

                String fileId = fileIdPrefix+KiteUtils.getRecoveryFileId
                        (estimateBlockId);
                Pair<Pair<Boolean,List<Microblog>>,Pair<Long,Long>>
                        searchResults = searchHDFSBlock(k, time, parentDir,
                        fileId);
                boolean searchSucceed = searchResults.getKey().getKey();
                if(searchSucceed) {
                    if(condition == null) {
                        latestK = searchResults.getKey().getValue();
                    } else {
                        List<Microblog> tmpList = searchResults.getKey().
                                getValue();
                        latestK = new ArrayList<>();
                        for(Microblog microblog: tmpList)
                            if(microblog.match(condition))
                                latestK.add(microblog);
                    }
                    if(latestK.size() < k) {
                        int blockId = estimateBlockId-1;
                        if(blockId >= 0) {
                            int restCount = k - latestK.size();
                            fileId = fileIdPrefix + KiteUtils.getRecoveryFileId
                                    (blockId);

                            List<Microblog> restK = getLatestKFromHDFSBlock(
                                    restCount,null, parentDir, fileId);
                            if(condition == null)
                                latestK.addAll(restK);
                            else {
                                for(Microblog microblog: restK)
                                    if(microblog.match(condition))
                                        latestK.add(microblog);
                            }
                        }
                        return latestK;
                    }
                    else
                        return latestK;
                }
                else {
                    Long firstTimestamp = searchResults.getValue().getKey();
                    Long lastTimestamp = searchResults.getValue().getValue();
                    Long blockTimeWidthTmp = lastTimestamp-firstTimestamp;
                    blockTimeWidthCount++;
                    blockTimeWidth = blockTimeWidthCount==1?
                            blockTimeWidthTmp:blockTimeWidth+blockTimeWidthTmp;
                    blockTimeWidth /= blockTimeWidthCount;

                    if(latestTimestamp < firstTimestamp) {
                        high = estimateBlockId-1;
                        baseTimestamp = firstTimestamp;
                    }
                    else if(latestTimestamp > lastTimestamp) {
                        low = estimateBlockId + 1;
                    }
                }
            }
        }
        return null;
    }

    private Pair<Pair<Boolean,List<Microblog>>,Pair<Long,Long>> searchHDFSBlock(
            int k, TemporalPeriod time, String parentDir, String fileId) {
        byte [] fileBytes = KiteUtils.readHDFSBlock(parentDir, fileId);
        if (fileBytes != null)
            return getLatestKFromBytes(k, time, fileBytes);
        else
            return null;
    }

    private Pair<Pair<Boolean,Microblog>,Pair<Long,Long>> searchHDFSBlock(
            Long id, String parentDir, String fileId) {

        byte [] fileBytes = KiteUtils.readHDFSBlock(parentDir, fileId);
        if(fileBytes != null)
            return getLatestKFromBytes(id,fileBytes);
        else
            return null;
    }

    private Pair<Pair<Boolean,Microblog>,Pair<Long,Long>> getLatestKFromBytes(long id,
                                                                              byte [] bytes) {
        ByteStream byteStream = new ByteStream(bytes);
        Microblog microblog;
        Long firstId = null, lastId = null;
        while ((microblog = Microblog.deserialize(byteStream, stream
                .getScheme()))!= null) {
            if(microblog.getId() == id) {
                //successful search
                return new Pair<>(new Pair<>(true,microblog),null);
            }
            else {
                if(firstId == null)
                    firstId = microblog.getId();
                lastId = microblog.getId();
            }
        }
        return new Pair<>(new Pair<>(false,null),new Pair<>(firstId,lastId));
    }

    private List<Microblog> getLatestKFromHDFSBlock(int k, TemporalPeriod time,
            String parentDir, String fileId) {

        byte [] fileBytes = KiteUtils.readHDFSBlock(parentDir, fileId);
        if(fileBytes != null)
            return getLatestKFromBytes(k, time, fileBytes)
                    .getKey().getValue();
        else
            return new ArrayList<>();
    }

    private Pair<Pair<Boolean,List<Microblog>>,Pair<Long,Long>> getLatestKFromBytes
            (int k, TemporalPeriod time, byte [] bytes) {
        //get latest "k" microblogs, within "time" in "bytes"
        //Assumption 1: microblogs in bytes are ordered by time from
        // older to more recent
        ByteStream byteStream = new ByteStream(bytes);
        List<Microblog> microblogs = new ArrayList<>(k);
        Microblog tmpMicroblog;

        Long firstTimestamp = null, lastTimestamp = null;
        while ((tmpMicroblog = Microblog.deserialize(byteStream, stream
                .getScheme()))!= null) {
            if(time == null || time.overlap(tmpMicroblog.getTimestamp())) {
                //successful search
                microblogs.add(tmpMicroblog);
                if(microblogs.size() > k)
                    microblogs.remove(0); //Assumption 1 above
            }
            else {
                if (firstTimestamp == null)
                    firstTimestamp = tmpMicroblog.getTimestamp();
                lastTimestamp = tmpMicroblog.getTimestamp();
            }
        }
        return new Pair<>(new Pair<>(microblogs.size()>0,microblogs),new Pair<>
                (firstTimestamp,lastTimestamp));
    }

    public StreamDataset(String name, StreamingDataSource dataSource, int
            memoryCapacity) {
        this.uniqueName = name;
        this.stream = dataSource;
        indexes = new ArrayList<>();
        indexesActivators = new ArrayList<>();
        preprocessor = stream.getPreprocessor();

        if(memoryCapacity > 0)
            streamData = new ArrayBigDataset(memoryCapacity);
        else
            streamData = new ArrayBigDataset();

        List<List<Microblog>> tempLst = new ArrayList<>();
        tempRecoveryDataList = Collections.synchronizedList(tempLst);

        //if existing stream, load recoveryBlkCounter and latest id
        Pair<String,String> dirFileidPair = getRecoveryFileIdPrefix();
        Pair<Integer, Long> existingIds = KiteUtils.loadStreamIds
                (dirFileidPair, stream.getScheme());
        recoveryBlkCounter = existingIds.getKey();
        currId = new IdGenerator(existingIds.getValue()+1);
    }

    public StreamDataset(String name, StreamingDataSource dataSource) {
        this(name, dataSource, -1);
    }

    public void startStreaming() {
        Thread streamingThread = new Thread(this);
        streamingThread.start();
    }

    private void streaming() {

        List<String> lines;
        List<Microblog> microblogs;

        while (!stopped) {
            insertionInProgress = true;
            try {
                lines = stream.getData();

                if (showEnable) {
                    String msg = getName() + " got " + lines.size() + " records " +
                            "from " + stream.toString() + ".";
                    System.out.println(msg);
                    KiteInstance.logShow(msg);
                }

                microblogs = preprocessor.preprocess(lines, currId);
            } catch (Exception e) {
                stopped = true;
                String errMsg = "Stream "+getName()+" has accidentally " +
                        "paused. The other stream end may not be responding " +
                        "correctly.";
                System.err.println();
                System.err.println(errMsg);
                KiteInstance.logError(errMsg);
                break;
            }
            //add to recovery buffer
            tempRecoveryDataList.add(microblogs);
            if(!appendingToRecoveryData) {
                Thread recoveryThread = new Thread(this);
                recoveryThread.start();
            }

            //insert in master buffer
            streamData.add(microblogs);

            ++streamBatchesCount;

            if(showEnable) {
                String msg = microblogs.size()+" records inserted in the " +
                        "stream " + getName();
                System.out.println(msg);
                KiteInstance.logShow(msg);
            }

            if(indexes.size() > 0) {
                for(indexInInsertion = 0; indexInInsertion < indexes.size();
                    ++indexInInsertion) {
                    if(indexesActivators.get(indexInInsertion)) {
                        MemoryIndex index = indexes.get(indexInInsertion);
                        dataInserted += index.insert(microblogs);

                        if (showEnable) {
                            String msg = "Index: " + index.
                                    getName() + " total inserted data size: "
                                    + index.getSize() + ", batches: " +
                                    streamBatchesCount+ ", in-memory data " +
                                    "size: " + index.getInMemorySize();
                            System.out.println(msg);
                            KiteInstance.logShow(msg);
                        }
                    }
                }
            }
            streamData.checkFlushing();
            insertionInProgress = false;
        }
    }

    public boolean inInsertion() {return insertionInProgress;}

    public boolean createIndexHash(Attribute attribute, String indexName, int
            indexCapacity, int numIndexSegments, boolean loadDiskIndex){

        indexes.add(new MemoryHashIndex(getIndexUniqueName(indexName),this,
                attribute, indexCapacity, numIndexSegments, loadDiskIndex));
        indexesActivators.add(false);

        return true;
    }

    public boolean createIndexHash(Attribute attribute, String indexName,
                                   boolean loadDiskIndex){
        return createIndexHash(attribute, indexName, ConstantsAndDefaults
                .MEMORY_INDEX_CAPACITY, ConstantsAndDefaults
                .MEMORY_INDEX_DEFAULT_SEGMENTS, loadDiskIndex);
    }

    public boolean createIndexSpatial(Attribute attribute, String indexName,
                                      SpatialPartitioner spatialPartitioner,
                                      boolean loadDiskIndex) {
        return createIndexSpatial(attribute, indexName, spatialPartitioner,
                ConstantsAndDefaults.MEMORY_INDEX_CAPACITY, ConstantsAndDefaults
                .MEMORY_INDEX_DEFAULT_SEGMENTS, loadDiskIndex);

    }
    public boolean createIndexSpatial(Attribute attribute, String indexName,
                                      SpatialPartitioner spatialPartitioner,
                                      int indexCapacity, int numIndexSegments,
                                      boolean loadDiskIndex) {
        if(!attribute.isSpatialAttribute())
            return false;

        indexes.add(new MemorySpatialIndex(getIndexUniqueName(indexName),
                spatialPartitioner, this, attribute, indexCapacity,
                numIndexSegments, loadDiskIndex));
        indexesActivators.add(false);

        return true;
    }

    public int size() {
        return streamData.size();
    }

    private String getDirectoryId() {
        return KiteInstance.getKiteRootDirectory()+  "kite_streams/"+
                uniqueName + "/";
    }

    public void decrementIndexesCount(long id) {
        streamData.decrementIndexCount(id);
    }

    public void incrementIndexCount(long id) {
        streamData.incrementIndexCount(id);
    }

    @Override
    public void run() {
        if(starting || resuming) {
            starting = false;
            resuming = false;

            try {
                streaming();
            } catch (OutOfMemoryError e) {
                String errMsg = "All active streams will be paused. " +
                        "Accidental OutOfMemoryError while streaming "+
                        this.getName()+"!!";
                errMsg += System.lineSeparator();
                errMsg += "Error: "+e.getMessage();
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
                KiteInstance.pauseAllActiveStreams();
            } catch (Exception e) {
                String errMsg = "Accidental error while streaming "+
                        this.getName()+"!!";
                errMsg += System.lineSeparator();
                errMsg += "Error: "+e.getMessage();
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
            }
        } else {
            try {
                appendRecoveryData();
            } catch (IOException e) {
                String errMsg = "Unable to write recovery data of stream "+
                        this.getName()+"!!";
                errMsg += System.lineSeparator();
                errMsg += "Error: "+e.getMessage();
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
            }
        }
    }

    private void appendRecoveryData() throws IOException {
        if(!appendingToRecoveryData) {
            appendingToRecoveryData = true;

            while (tempRecoveryDataList.size() > 0) {
                List<Microblog> currRecoveryList = tempRecoveryDataList.get(0);
                tempRecoveryDataList.remove(0);
                for (Microblog m : currRecoveryList) {
                    synchronized (recoveryDataLock) {
                        m.serialize(recoveryData, stream.getScheme());
                    }
                    //System.out.println("appending recovery id="+m.getId());
                }

                if (recoveryData.getWrittenBytes() >= ConstantsAndDefaults.
                        RECOVERY_BLK_SIZE_BYTES) {
                    Pair<String,String> dirFileidPair =
                            getRecoveryFileIdPrefix();
                    String parentDirectory = dirFileidPair.getKey();
                    String filePrefix = dirFileidPair.getValue();
                    String fileId = filePrefix + getNextRecoveryFileId();

                    KiteUtils.writeHDFSBlock(recoveryData, parentDirectory, fileId);

                    synchronized (recoveryDataLock) {
                        recoveryData.clear();
                        recoveryData = new ByteStream(ConstantsAndDefaults.
                                RECOVERY_BLK_SIZE_BYTES);
                    }
                }
            }
            appendingToRecoveryData = false;
        }
    }

    private String getNextRecoveryFileId() {
        String id = KiteUtils.getRecoveryFileId(recoveryBlkCounter);
        recoveryBlkCounter++;
        return id;
    }

    private Pair<String,String> getRecoveryFileIdPrefix() {
        String fileId = uniqueName + "_data";
        String parentDirectory = getDirectoryId();

        Pair<String,String> fileDir = new Pair<String,String>
                (parentDirectory, fileId);
        return fileDir;
    }

    public Scheme getScheme() {return stream.getScheme();}

    public Attribute getAttribute(String attributeName) {
        return getScheme().getAttribute(attributeName);
    }

    public String getName() {
        return uniqueName;
    }

    public void show() {
        showEnable = true;
    }
    public void unshow() {
        showEnable = false;
    }

    public boolean isShown() { return showEnable; }

    public MQLResults search(Query query, ArrayList<String> attributeNames) {
        MQLResults results = null;
        QueryType queryType = query.getQueryType();
        switch (queryType) {
            case UNDECIDED:
                //decide on the query type and send it for evaluation
                Pair<QueryType, Pair<Attribute, Object>> decidedQueryType =
                        decideOnQueryType (query);
                query.setQueryType(decidedQueryType.getKey());
                query.setPrimaryAttribute(decidedQueryType.getValue().getKey());
                query.setPrimaryAttributeValue(decidedQueryType.getValue()
                        .getValue());
                results = this.search(query, attributeNames);
                break;
            case UNINDEXED_TEMPORAL:
            case PURE_TEMPORAL:
                Condition condition = null;
                if(queryType == QueryType.UNINDEXED_TEMPORAL)
                    condition = query.getCondition();
                int k = query.getK();
                TemporalPeriod time = query.getTime();
                ArrayList<Microblog> latestK = streamData.getLatestK(k, time,
                        condition);
                if(latestK.size() < k) {
                    //search disk
                    int restCount = k-latestK.size();
                    List<Microblog> rest = getLatestKFromDisk(restCount,
                            time, condition);
                    if(rest != null)
                        latestK.addAll(rest);
                }
                results = new MQLResults(latestK, attributeNames);
                break;
            case HASH_TEMPORAL:
                MemoryHashIndex hashIndex = (MemoryHashIndex) getIndex(query
                        .getPrimaryAttribute());


                if(hashIndex != null) {
                    List<String> keyLst = query.getPrimaryAttribute()
                          .getStringListValue(query.getPrimaryAttributeValue());
                    String key = keyLst != null? keyLst.get(0): null;

                    List<Long> mIds = hashIndex.search(key, query, query
                            .getTime());

                    List<Microblog> resultsMicroblogs = new ArrayList<>();
                    for(Long mId:mIds) {
                        Microblog microblog = getRecord(mId);
                        if(query.isSingleSearchAttribute()) {
                            resultsMicroblogs.add(microblog);
                            if(resultsMicroblogs.size() >= query.getK())
                                break;
                        }
                        else if(microblog.match(query.getCondition())) {
                            resultsMicroblogs.add(microblog);
                            if(resultsMicroblogs.size() >= query.getK())
                                break;
                        }
                    }
                    if(resultsMicroblogs.size() < query.getK()) {
                        //search corresponding disk index
                        int restCount = query.getK()-resultsMicroblogs.size();

                        List<Microblog> diskMicroblogs = hashIndex.searchDisk
                                (restCount, key, query, query.getTime());
                        if(diskMicroblogs != null) {
                            resultsMicroblogs.addAll(diskMicroblogs);
                        }
                    }
                    results = new MQLResults(resultsMicroblogs, attributeNames);
                }
                break;
            case SPATIAL_RANGE_TEMPORAL:
                MemorySpatialIndex spatialIndex = (MemorySpatialIndex)
                        getIndex(query.getPrimaryAttribute());
                if(spatialIndex != null) {
                    List<GeoLocation> keyLst = query.getPrimaryAttribute()
                            .getGeoListValue(query.getPrimaryAttributeValue());
                    GeoLocation key = keyLst != null? keyLst.get(0): null;

                    List<Long> mIds = spatialIndex.search(key, query,
                                                            query.getTime());
                    List<Microblog> resultsMicroblogs = new ArrayList<>();
                    for(Long mId:mIds) {
                        Microblog microblog = getRecord(mId);
                        if(query.isSingleSearchAttribute()) {
                            resultsMicroblogs.add(microblog);
                            if(resultsMicroblogs.size() >= query.getK())
                                break;
                        }
                        else if(microblog.match(query.getCondition())) {
                            resultsMicroblogs.add(microblog);
                            if(resultsMicroblogs.size() >= query.getK())
                                break;
                        }
                    }
                    if(resultsMicroblogs.size() < query.getK()) {
                        //search corresponding disk index
                        int restCount = query.getK()-resultsMicroblogs.size();
                        List<Microblog> diskMicroblogs = spatialIndex.searchDisk
                                (restCount, key, query, query.getTime());
                        resultsMicroblogs.addAll(diskMicroblogs);
                    }
                    results = new MQLResults(resultsMicroblogs, attributeNames);
                }
                break;
            case SPATIAL_KNN_TEMPORAL:
                //skip for now
                break;
            default:
                //unknown query type
                break;
        }
        return results;
    }

    private MemoryIndex getIndex(Attribute attribute) {
        for(MemoryIndex index: indexes)
            if(index.getIndexedAttribute().equals(attribute))
                return index;
        return null;
    }

    private Pair<QueryType, Pair<Attribute, Object>> decideOnQueryType
                                        (Query query) {
        boolean hasOR = query.getCondition().hasOR();
        List<Attribute> searchAttributes = query.getCondition()
                .getSearchAttributes();
        QueryType queryType = null;
        Attribute primaryAttribute = null;
        Object primaryAttributeVal = null;

        if(hasOR) {
            //do not use indexes for now
            //ToDo: handle OR queries with potential multiple index access
            queryType = QueryType.UNINDEXED_TEMPORAL;
        } else if(searchAttributes.size() > 0) {
            List<Attribute> indexedAttributes = new ArrayList<>();
            for(int i = 0; i < indexes.size(); ++i) {
                if(indexesActivators.get(i))
                    indexedAttributes.add(indexes.get(i).getIndexedAttribute());
            }

            //get the list of search attribute that are indexed
            List<Attribute> searchIndexedAttributes = new ArrayList<>();
            List<Attribute> list1, list2;
            if(searchAttributes.size() < indexedAttributes.size()) {
                list1 = searchAttributes;
                list2 = indexedAttributes;
            } else {
                list1 = indexedAttributes;
                list2 = searchAttributes;
            }
            for(Attribute attribute:list1)
                if(list2.contains(attribute))
                    searchIndexedAttributes.add(attribute);

            if(searchIndexedAttributes.size() == 0) {
                //the search attributes are not indexed
                queryType = QueryType.UNINDEXED_TEMPORAL;
            } else if(searchIndexedAttributes.size() == 1) {
                //single search attribute is indexed
                if(searchIndexedAttributes.get(0).isSpatialAttribute()) {
                    queryType = QueryType.SPATIAL_RANGE_TEMPORAL;
                } else {
                    queryType = QueryType.HASH_TEMPORAL;
                }
                primaryAttribute = searchIndexedAttributes.get(0);
            } else {//multiple search attributes are indexed
                for (Attribute attribute: searchIndexedAttributes) {
                    if(!attribute.isSpatialAttribute()) {
                        queryType = QueryType.HASH_TEMPORAL;
                        primaryAttribute = attribute;
                    }
                }
                if(primaryAttribute == null) {
                    queryType = QueryType.SPATIAL_RANGE_TEMPORAL;
                    primaryAttribute = searchIndexedAttributes.get(0);
                }
            }
        } else {
            queryType = QueryType.PURE_TEMPORAL;
        }

        if(primaryAttribute != null) {
            primaryAttributeVal = query.getCondition().getValue
                    (primaryAttribute);
        }

        return new Pair<>(queryType, new Pair<>(primaryAttribute,
                primaryAttributeVal));
    }

    public boolean activateIndex(String indexName) {
        String indexUniqueName = getIndexUniqueName(indexName);
        for(int i = indexes.size()-1; i >= 0; --i) {
            if(indexes.get(i).getName().compareTo(indexUniqueName)==0){
                indexesActivators.set(i,true);
                return true;
            }
        }
        return false;
    }

    public boolean deactivateIndex(String indexName) {
        String indexUniqueName = getIndexUniqueName(indexName);
        for(int i = indexes.size()-1; i >= 0; --i) {
            if(indexes.get(i).getName().compareTo(indexUniqueName)==0){
                indexesActivators.set(i,false);
                return true;
            }
        }
        return false;
    }

    private String getIndexUniqueName(String indexName) {
        return uniqueName+"_"+indexName;
    }

    public boolean destroyIndex(String indexName) {
        String indexUniqueName = getIndexUniqueName(indexName);
        for(int i = indexes.size()-1; i >= 0; --i) {
            if(indexes.get(i).getName().compareTo(indexUniqueName)==0){
                indexesActivators.set(i,false);
                while (indexInInsertion == i)
                    KiteUtils.busyWait(50);
                //ToDo: check for races among threads here (if any)
                indexes.get(i).destroy();
                indexes.remove(i);
                indexesActivators.remove(i);
                return true;
            }
        }
        return false;
    }

    public boolean destroyAllIndexes() {
        boolean result = true;
        for(int i = indexes.size()-1; i >= 0; --i) {
            indexesActivators.set(i,false);
            while (indexInInsertion == i)
                KiteUtils.busyWait(50);
            //ToDo: check for races among threads here (if any)
            indexes.get(i).destroy();
            indexes.remove(i);
            indexesActivators.remove(i);
            result = result && true;
        }
        return result;
    }

    public void stop() {
        stopped = true;
    }

    public void resume() {
        stopped = false;
        resuming = true;
        startStreaming();
    }
}
