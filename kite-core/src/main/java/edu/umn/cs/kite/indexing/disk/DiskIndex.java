package edu.umn.cs.kite.indexing.disk;

import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.indexing.memory.MemoryIndexSegment;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.TemporalPeriod;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by amr_000 on 8/1/2016.
 */
public abstract class DiskIndex<K,V> {

    protected String indexUniqueName;
    protected StreamDataset stream;
    protected List<DiskIndexSegment> indexSegments = new ArrayList<>();
    protected List<TemporalPeriod> indexSegmentsTime = new ArrayList<>();
    protected ByteStream indexSegmentsTimeSerializer = new ByteStream (8000);

    public String getIndexUniqueName() {
        return indexUniqueName;
    }

    public abstract boolean addSegment(MemoryIndexSegment memorySegment,
                                       TemporalPeriod p);
    public abstract ArrayList<V> search (K key, int k, TemporalPeriod period,
                                         Query q);

    public abstract void destroy();

    protected boolean appendToDisk(TemporalPeriod p) {
        indexSegmentsTimeSerializer = KiteUtils.checkByteStreamCapacity
                (indexSegmentsTimeSerializer, Long.BYTES*2);
        p.serialize(indexSegmentsTimeSerializer);
        String dir = getDirectoryId();
        String fileId = getSegmentsFileId();
        try {
            KiteUtils.writeHDFSBlock(indexSegmentsTimeSerializer,
                    dir, fileId);
            return true;
        } catch (IOException e) {
            String errMsg = "Unable to write to HDFS file "+dir+fileId;
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return false;
        }
    }

    protected void loadSegments() {
        byte [] segmentsTimesBytes = KiteUtils.readHDFSBlock(getDirectoryId()
                , getSegmentsFileId());
        if(segmentsTimesBytes != null) {
            byte[] copySegmentsTimesBytes = Arrays.copyOf(segmentsTimesBytes,
                    segmentsTimesBytes.length);
            indexSegmentsTimeSerializer = KiteUtils.checkByteStreamCapacity
                    (indexSegmentsTimeSerializer, segmentsTimesBytes.length);
            indexSegmentsTimeSerializer.write(segmentsTimesBytes);

            //load segments
            ByteStream tmpSerializer = new ByteStream(copySegmentsTimesBytes);
            TemporalPeriod period = TemporalPeriod.deserialize(tmpSerializer);
            while (period != null) {
                DiskIndexSegment segment = loadNextSegment();
                indexSegments.add(segment);
                indexSegmentsTime.add(period);
                period = TemporalPeriod.deserialize(tmpSerializer);
            }
        }
    }

    protected abstract DiskIndexSegment loadNextSegment();

    private String getDirectoryId() {
        return ConstantsAndDefaults.DISK_INDEXES_ROOT_DIRECTORY +
                getIndexUniqueName() +"/";
    }

    private String getSegmentsFileId() {
        return getIndexUniqueName()+"_segments";
    }
}
