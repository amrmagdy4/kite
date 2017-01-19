package edu.umn.cs.kite.indexing.disk.spatial;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.indexing.disk.DirectoryEntry;
import edu.umn.cs.kite.indexing.disk.DiskIndexSegment;
import edu.umn.cs.kite.indexing.memory.spatial.MemorySpatialIndexSegment;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialIndex;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartition;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartitioner;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.microblogs.Microblog;
import edu.umn.cs.kite.util.serialization.ByteStream;
import edu.umn.cs.kite.util.serialization.Serializer;
import javafx.util.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.cache.Cache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by amr_000 on 11/26/2016.
 */
public class DiskSpatialIndexSegment extends SpatialIndex implements
        DiskIndexSegment<GeoLocation> {
    private String uniqueName;
    private String parentIndexUniqueName;
    private StreamDataset stream;
    private Attribute indexedAttribute;

    private String directoryFileFolder;
    private String directoryFileId;
    private String spatialPartitionerFilePath;
    private int counter = 0;
    private ArrayList<DirectoryEntry> directoryEntries;

    public DiskSpatialIndexSegment (String parentName, String uniqueName,
                                    SpatialPartitioner partitioner,
                                    MemorySpatialIndexSegment memorySpatialIndex,
                                    StreamDataset stream,
                                    Attribute indexedAttribute)
            throws IOException {
        this.uniqueName = uniqueName;
        this.parentIndexUniqueName = parentName;
        this.spatialPartitioner = partitioner;
        this.spatialPartitionerFilePath = getSpatialPartitionerFileId();
        this.stream = stream;
        this.indexedAttribute = indexedAttribute;

        if(memorySpatialIndex != null)
            flushMemoryIndex(memorySpatialIndex, stream);

        directoryFileFolder = getDirectoryId();
        directoryFileId = getIndexDirectoryFileId();
    }

    private String getIndexDirectoryFileId() {
        return uniqueName+"_directory";
    }

    private boolean flushMemoryIndex(MemorySpatialIndexSegment memorySpatialIndex,
                                     StreamDataset stream)
            throws IOException {

        //write memory index entry at a time
        Iterator<Cache.Entry<Integer,SpatialPartition>>
                memoryIndexContentsItr = memorySpatialIndex.getEntriesIterator();

        int singleFileSize = ConstantsAndDefaults.FILE_BLK_SIZE_BYTES;

        //flush the spatial partitioner
        ByteStream spatialPartitionerBytes = spatialPartitioner.serialize();
        String directoryFileParent = getDirectoryId();
        String directoryFileId =  getSpatialPartitionerFileId();
        spatialPartitionerFilePath = KiteUtils.writeHDFSBlock
                (spatialPartitionerBytes, directoryFileParent, directoryFileId);

        ByteStream indexSerializedBytes = new ByteStream(singleFileSize);

        int serializedBytesCount = 0;
        ArrayList<DirectoryEntry> serializedKeys = new ArrayList<>();

        directoryEntries = new ArrayList<>();
        long offset = 0;
        while(memoryIndexContentsItr.hasNext()) {
            Cache.Entry<Integer,SpatialPartition> entry = memoryIndexContentsItr
                    .next();

            int entryInd = 0;
            while (entryInd >= 0) {
                Pair<Integer,Integer> result = Serializer.serializeSpatialEntry
                        (entry, stream, indexSerializedBytes, entryInd);
                int bytesLength = result.getKey();
                int continuingEntryInd = result.getValue();
                serializedBytesCount += bytesLength;
                entryInd = continuingEntryInd;

                serializedKeys.add(new DirectoryEntry(null,entry.getKey(),
                        offset, bytesLength));
                offset = serializedBytesCount;

                //accumalted block size, flush it to HDFS
                if (serializedBytesCount >= singleFileSize) {
                    ArrayList<DirectoryEntry> blockDirectoryEntries =
                            writeBlockWithKeysGeo(serializedKeys,
                                    indexSerializedBytes);
                    directoryEntries.addAll(blockDirectoryEntries);
                    serializedKeys.clear();
                    serializedBytesCount = 0;
                    offset = 0;
                    indexSerializedBytes = new ByteStream(singleFileSize);
                }
            }
        }
        //write the final block
        ArrayList<DirectoryEntry> blockDirectoryEntries =
                writeBlockWithKeysGeo(serializedKeys, indexSerializedBytes);
        directoryEntries.addAll(blockDirectoryEntries);

        //write directory block
        writeDirectoryFile(directoryEntries);

        return true;
    }

    private String getSpatialPartitionerFileId() {
        return uniqueName+"_spatialPartitioner";
    }

    private String writeDirectoryFile(ArrayList<DirectoryEntry>
                                              directoryEntries)
            throws IOException {

        directoryFileFolder = getDirectoryId();
        directoryFileId =  getIndexDirectoryFileId();

        //serialize directory entries
        String tmp_string = directoryFileFolder + directoryFileId;
        double charByteRatio = tmp_string.getBytes().length*1.0/ tmp_string
                .length();

        int capacity = (int)(directoryEntries.size()*(uniqueName.length()+5
                +Integer.BYTES*2+14)*charByteRatio+200);
        ByteStream byteStream = new ByteStream(capacity);

        byteStream.write(directoryEntries.size());
        for(int i = 0; i < directoryEntries.size(); ++i) {
            byteStream.write(directoryEntries.get(i).getKeyGeo());
            byteStream.write(directoryEntries.get(i).getFilePath());
            byteStream.write(directoryEntries.get(i).getOffset());
            byteStream.write(directoryEntries.get(i).getLength());
        }

        KiteUtils.writeHDFSBlock(byteStream, directoryFileFolder,
                directoryFileId);
        return directoryFileFolder+directoryFileId;
    }

    private ArrayList<DirectoryEntry> loadDirectory(String dirFileParent,
                                                    String dirFileId) {
        byte[] directoryBytes = KiteUtils.readHDFSBlock(dirFileParent,
                dirFileId);

        ByteStream byteStream = new ByteStream(directoryBytes);

        int listLen = byteStream.readInt();
        ArrayList<DirectoryEntry> entries = new ArrayList<>(listLen);
        for(int i = 0; i < listLen; ++i) {
            Integer key = byteStream.readInt();
            String filePath = byteStream.readString();
            long offset = byteStream.readLong();
            int len = byteStream.readInt();

            DirectoryEntry entry=new DirectoryEntry(filePath, key, offset, len);
            entries.add(entry);
        }
        byteStream.clear();
        return entries;
    }

    private ArrayList<DirectoryEntry> writeBlockWithKeysGeo(
            ArrayList<DirectoryEntry> serializedKeys, ByteStream
            indexSerializedBytes)
            throws IOException {

        String parentDirectory = getDirectoryId();
        String fileId = getNextFileId();

        KiteUtils.writeHDFSBlock(indexSerializedBytes, parentDirectory, fileId);

        ArrayList<DirectoryEntry> directoryEntries = new ArrayList<>();
        for(DirectoryEntry entry: serializedKeys) {
            directoryEntries.add(new DirectoryEntry(fileId, entry.getKeyGeo(),
                    entry.getOffset(), entry.getLength()));
        }
        return directoryEntries;
    }

    private String getNextFileId() {
        String counterStr = KiteUtils.int2str(counter,4);
        ++counter;
        return uniqueName + "_" + counterStr;
    }

    private String getDirectoryId() {
        return ConstantsAndDefaults.DISK_INDEXES_ROOT_DIRECTORY +
                parentIndexUniqueName +"/" + uniqueName + "/";
    }

    @Override
    public ArrayList<Microblog> search(GeoLocation key, int k, Query query) {
        ArrayList<Microblog> results = new ArrayList<>();

        if(directoryEntries == null || directoryEntries.isEmpty())
            directoryEntries = loadDirectory(directoryFileFolder,
                    directoryFileId);

        if(query.isSpatialRange()) {
            Rectangle queryRange = key.isPoint()? query.getParam_Rectangle
                    ("SpatialRange"): (Rectangle)key;

            if(this.spatialPartitioner == null)
                this.spatialPartitioner = deserializeSpatialPartitioner(
                        spatialPartitionerFilePath);
            List<Integer> partitionIds = this.spatialPartitioner.overlap
                    (queryRange);

            for(int i = 0; i < directoryEntries.size() && results.size() < k;
                ++i) {
                Integer paritionId = directoryEntries.get(i).getKeyGeo();
                if(paritionId != null && partitionIds.contains(paritionId)) {
                    Pair<Integer,ArrayList<Microblog>> microblogs =
                            readPartitionMicroblogs (directoryEntries.get(i),
                                    queryRange);
                    ArrayList<Microblog> records = microblogs.getValue();
                    if(query.isSingleSearchAttribute())
                        results.addAll(records);
                    else {
                        for(Microblog record: records)
                            if(record.match(query.getCondition()))
                                results.add(record);
                    }
                }
            }
        } else if(query.isSpatialKNN()) {
            //ToDo: implement kNN
        }
        return results;
    }


    @Override
    public void destroy() {
        //ToDo: erase disk data of this segment
    }

    @Override
    public boolean discardInMemoryDirectory() {
        if(directoryEntries != null) {
            directoryEntries.clear();
            directoryEntries = null;
            return true;
        } else return false;
    }

    private SpatialPartitioner deserializeSpatialPartitioner(
            String spatialPartitionerFilePath) {
        String hdfsPath = getDirectoryId()+spatialPartitionerFilePath;

        FileSystem hdfs = KiteInstance.hdfs();
        try {
            FSDataInputStream block = hdfs.open(new Path(hdfsPath),
                    ConstantsAndDefaults.BUFFER_SIZE_BYTES);

            int len = block.readInt();
            byte [] serializedData = new byte[len];

            int read = block.read(serializedData, 0, len);
            if(read <= 0)
                return null;

            return SpatialPartitioner.deserialize(serializedData);
        } catch (IOException e) {
            String errMsg = "Unable to read HDFS path "+hdfsPath;
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return null;
        }
    }

    private Pair<Integer,ArrayList<Microblog>> readPartitionMicroblogs(
            DirectoryEntry dirEntry, Rectangle queryRange) {
        String hdfsPath = getDirectoryId()+dirEntry.getFilePath();
        Integer partitionId = dirEntry.getKeyGeo();
        long offset = dirEntry.getOffset();
        int length = dirEntry.getLength();

        FileSystem hdfs = KiteInstance.hdfs();
        try {
            FSDataInputStream block = hdfs.open(new Path(hdfsPath),
                    ConstantsAndDefaults.BUFFER_SIZE_BYTES);
            byte [] keyData = new byte[length];
            int read = block.read(offset,keyData,0,length);
            if(read <= 0)
                return null;

            Pair<Integer,ArrayList<Microblog>> singleHashEntry =
                    Serializer.deserializeSpatialEntry(keyData,
                            stream.getScheme());

            return new Pair<>(singleHashEntry.getKey(),
                    filterSpatialRange(singleHashEntry.getValue(), queryRange));
        } catch (IOException e) {
            String errMsg = "Unable to read HDFS path "+hdfsPath;
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return null;
        }
    }

    private ArrayList<Microblog> filterSpatialRange(ArrayList<Microblog>
                                 microblogs, Rectangle queryRange) {
        ArrayList<Microblog> filtered = new ArrayList<>();
        for(Microblog microblog: microblogs) {

            if(queryRange.isInside(microblog.getLocation(indexedAttribute)))
                filtered.add(microblog);
        }
        return filtered;
    }
}
