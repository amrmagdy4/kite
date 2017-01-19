package edu.umn.cs.kite.indexing.disk;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.indexing.memory.MemoryHashIndexSegment;
import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.KiteUtils;
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
import java.util.Hashtable;
import java.util.Iterator;

/**
 * Created by amr_000 on 8/25/2016.
 */
public class DiskHashIndexSegment implements DiskIndexSegment<String> {

    private String uniqueName;
    private String parentIndexUniqueName;
    private StreamDataset stream;

    private String directoryFileFolder;
    private String directoryFileId;
    private int counter = 0;
    private ArrayList<DirectoryEntry> directoryEntries;

    public DiskHashIndexSegment(String parentName, String uniqueName,
            MemoryHashIndexSegment memoryHashIndex, StreamDataset stream)
            throws IOException {
        this.uniqueName = uniqueName;
        this.parentIndexUniqueName = parentName;
        this.stream = stream;

        if(memoryHashIndex != null)
            flushMemoryIndex(memoryHashIndex, stream);

        directoryFileFolder = getDirectoryId();
        directoryFileId = getIndexDirectoryFileId();
    }

    private boolean flushMemoryIndex(MemoryHashIndexSegment memoryHashIndex,
                                     StreamDataset stream)
            throws IOException {
        //write memory index entry at a time
        Iterator<Cache.Entry<String,ArrayList<Long>>>
                memoryIndexContentsItr = memoryHashIndex.getEntriesIterator();

        int singleFileSize = ConstantsAndDefaults.FILE_BLK_SIZE_BYTES;
        ByteStream indexSerializedBytes = new ByteStream(singleFileSize);

        int serializedBytesCount = 0;
        ArrayList<DirectoryEntry> serializedKeys = new ArrayList<>();

        directoryEntries = new ArrayList<>();
        long offset = 0;
        while(memoryIndexContentsItr.hasNext()) {
            Cache.Entry<String,ArrayList<Long>> entry = memoryIndexContentsItr
                    .next();

            int entryInd = 0;
            while (entryInd >= 0) {
                Pair<Integer,Integer> result = Serializer.serializeHashEntry
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
                          writeBlockWithKeys(serializedKeys, indexSerializedBytes);
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
                writeBlockWithKeys(serializedKeys, indexSerializedBytes);
        directoryEntries.addAll(blockDirectoryEntries);

        //write directory block
        writeDirectoryFile(directoryEntries);

        return true;
    }

    private String writeDirectoryFile(ArrayList<DirectoryEntry>
                                              directoryEntries)
            throws IOException {

        directoryFileFolder = getDirectoryId();
        directoryFileId = getIndexDirectoryFileId();

        //serialize directory entries
        String tmp_string = directoryFileId + directoryFileId;
        double charByteRatio = tmp_string.getBytes().length*1.0/ tmp_string
                .length();

        int capacity = (int)(directoryEntries.size()*(uniqueName.length()+5
                +Integer.BYTES*2+14)*charByteRatio+200);
        ByteStream byteStream = new ByteStream(capacity);

        byteStream.write(directoryEntries.size());
        for(int i = 0; i < directoryEntries.size(); ++i) {
            byteStream.write(directoryEntries.get(i).getKeyStr());
            byteStream.write(directoryEntries.get(i).getFilePath());
            byteStream.write(directoryEntries.get(i).getOffset());
            byteStream.write(directoryEntries.get(i).getLength());
        }

        KiteUtils.writeHDFSBlock(byteStream, directoryFileFolder,
                directoryFileId);
        return directoryFileFolder+directoryFileId;
    }

    private String getIndexDirectoryFileId() {
        return uniqueName+"_directory";
    }

    private ArrayList<DirectoryEntry> writeBlockWithKeys(
            ArrayList<DirectoryEntry> serializedKeys, ByteStream
            indexSerializedBytes)
            throws IOException {

        String parentDirectory = getDirectoryId();
        String fileId = getNextFileId();

        KiteUtils.writeHDFSBlock(indexSerializedBytes, parentDirectory, fileId);

        ArrayList<DirectoryEntry> directoryEntries = new ArrayList<>();
        for(DirectoryEntry entry: serializedKeys) {
            directoryEntries.add(new DirectoryEntry(fileId, entry.getKeyStr(),
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
                parentIndexUniqueName + "/" + uniqueName + "/";
    }

    @Override
    public ArrayList<Microblog> search(String key, int k, Query query) {

        ArrayList<Microblog> results = new ArrayList<>();
        if(directoryEntries == null || directoryEntries.isEmpty())
            directoryEntries = loadDirectory(directoryFileFolder,
                                                directoryFileId);
        for(int i = 0; i < directoryEntries.size() && results.size() < k; ++i) {
            if(directoryEntries.get(i).getKeyStr() != null && directoryEntries
                    .get(i).getKeyStr().compareTo(key) == 0) {
                Hashtable<String,ArrayList<Microblog>>
                    microblogs = readKeyMicroblogs(directoryEntries.get(i));
                ArrayList<Microblog> records = microblogs.get(key);

                if(query.isSingleSearchAttribute()) {
                    results.addAll(records);
                } else {
                    for(Microblog record: records)
                        if(record.match(query.getCondition()))
                            results.add(record);
                }
            }
        }
        while (results.size() > k)
            results.remove(results.size()-1);
        return results;
    }

    private ArrayList<DirectoryEntry> loadDirectory(String dirFileParent,
                                                    String dirFileId) {
        byte[] directoryBytes = KiteUtils.readHDFSBlock(dirFileParent,
                dirFileId);

        ByteStream byteStream = new ByteStream(directoryBytes);

        int listLen = byteStream.readInt();

        ArrayList<DirectoryEntry> entries = new ArrayList<>(listLen);
        for(int i = 0; i < listLen; ++i) {
            String key = byteStream.readString();
            String filePath = byteStream.readString();
            long offset = byteStream.readLong();
            int len = byteStream.readInt();

            DirectoryEntry entry=new DirectoryEntry(filePath, key, offset, len);
            entries.add(entry);
        }
        byteStream.clear();
        return entries;
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


    private Hashtable<String,ArrayList<Microblog>> readKeyMicroblogs(
            DirectoryEntry dirEntry) {
        String hdfsPath = getDirectoryId()+dirEntry.getFilePath();
        String key = dirEntry.getKeyStr();
        long offset = dirEntry.getOffset();
        int length = dirEntry.getLength();

        Hashtable<String,ArrayList<Microblog>> blockEntries = new Hashtable<>();
        FileSystem hdfs = KiteInstance.hdfs();
        try {
            FSDataInputStream block = hdfs.open(new Path(hdfsPath),
                    ConstantsAndDefaults.BUFFER_SIZE_BYTES);
            byte [] keyData = new byte[length];
            int read = block.read(offset,keyData,0,length);
            if(read <= 0)
                return null;
            Hashtable<String,ArrayList<Microblog>> singleHashEntry =
                   Serializer.deserializeHashEntry(keyData, stream.getScheme());
            //java.nio.BufferUnderflowException thrown here

            return singleHashEntry;
        } catch (IOException e) {
            String errMsg = "Unable to read HDFS path "+hdfsPath;
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return null;
        }
    }
}
