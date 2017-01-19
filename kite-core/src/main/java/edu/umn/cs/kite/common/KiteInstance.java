package edu.umn.cs.kite.common;

import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.querying.MQL;
import edu.umn.cs.kite.querying.MQLResults;
import edu.umn.cs.kite.querying.metadata.IndexMetadataEntry;
import edu.umn.cs.kite.querying.metadata.MetadataEntry;
import edu.umn.cs.kite.querying.metadata.StreamMetadataEntry;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.KiteUtils;
import edu.umn.cs.kite.util.serialization.ByteStream;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.Ignite;

import java.io.*;
import java.time.Instant;
import java.util.*;

/**
 * Created by amr_000 on 8/1/2016.
 */
public class KiteInstance {
    private static KiteLaunchTool kite;
    private static FileSystem hdfs;

    private static String hdfsRootDirectory = "/";
    private static String hdfsHost = "hdfs://host:port";
    private static String hdfsUsername = "amr";
    private static String hdfsGroupname = null;

    private static ArrayList<MetadataEntry> metadata = new ArrayList<>();
    //private static ByteStream metadataSerializer = new ByteStream(7500);
    private static Hashtable<String, StreamDataset> streams = new Hashtable<>();

    private static BufferedWriter outLog, errorLog,showLog;
    private static Timer logTimer;
    private static String logsDirectory = "";

    public static Ignite getCluster() {
        return kite.getCluster();
    }

    public static void initSettings(KiteLaunchTool kite) {
        initSettings(kite,"kite.settings");
    }

    public static void initSettings(KiteLaunchTool kite, String
            settingsFilePath){
        readSettingsFile(settingsFilePath);
        setKite(kite);
        setHDFS();
        ConstantsAndDefaults.DISK_INDEXES_ROOT_DIRECTORY =
                                getKiteRootDirectory() +  "kite_disk_indexes/";

        //read existing streams and indexes
        loadMetadata();

        createLogs();
    }

    private static void createLogs() {
        String timestamp = getNowTimestamp();

        try {
            outLog = new BufferedWriter(new FileWriter
                    (logsDirectory+"kite-out-log-"+timestamp));
        } catch (IOException e) {
            String errMsg = "Unable to create log at " + logsDirectory +
                    "kite-out-log-"+timestamp;
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
        }

        try {
            errorLog = new BufferedWriter(new FileWriter
                    (logsDirectory + "kite-error-log-" + timestamp));
        } catch (IOException e) {
            String errMsg = "Unable to create log at " + logsDirectory +
                    "kite-error-log-"+timestamp;
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
        }

        try {
            showLog = new BufferedWriter(new FileWriter
                    (logsDirectory + "kite-show-log-" + timestamp));
        } catch (IOException e) {
            String errMsg = "Unable to create log at " + logsDirectory +
                    "kite-show-log-"+timestamp;
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
        }

        logTimer = new Timer(true);
        long timerPeriod = ConstantsAndDefaults.LOG_PERIOD_MILLISECONDS;
        logTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                KiteInstance.flushLogs();
            }
        }, timerPeriod, timerPeriod);
    }

    private static void flushLogs() {
        try {
            if(outLog != null)
                outLog.flush();
            if(errorLog != null)
                errorLog.flush();
            if(showLog != null)
                showLog.flush();
        } catch (IOException e) {
        }
    }

    public static void logOut(String logStr) {
        if(outLog != null) {
            try {
                outLog.write(getNowTimestamp()+":"+logStr+System.lineSeparator());
            } catch (IOException e) {
            }
        }
    }

    public static void logError(String logStr) {
        if(errorLog != null) {
            try {
                errorLog.write(getNowTimestamp()+":"+logStr+System.lineSeparator());
            } catch (IOException e) {
            }
        }
    }

    public static void logShow(String logStr) {
        if(showLog != null) {
            try {
                showLog.write(getNowTimestamp()+":"+logStr+System.lineSeparator());
            } catch (IOException e) {
            }
        }
    }

    private static void readSettingsFile(String settingsFilePath) {
        try {
            Properties prop = new Properties();
            //load a properties configuration file
            prop.load(new FileInputStream(settingsFilePath));

            String tmpHdfsRootDirectory = prop.getProperty("hdfsRootDirectory");
            String tmpHdfsHost = prop.getProperty("hdfsHost");
            String tmpHdfsUsername = prop.getProperty("hdfsUsername");
            String tmpHdfsGroupname = prop.getProperty("hdfsGroupname");

            String tmpK = prop.getProperty("queryAnswerSize");
            String tmpTime = prop.getProperty("queryTimeMinutes");

            String tmpMemoryIndexCapacity = prop.getProperty
                    ("memoryIndexCapacity");
            String tmpMemoryIndexNumSegments = prop.getProperty
                    ("memoryIndexNumSegments");

            String tmpLogsDirectory = prop.getProperty("logsDirectory");

            if(tmpHdfsRootDirectory != null && tmpHdfsRootDirectory.compareTo
                    ("") != 0)
                hdfsRootDirectory = tmpHdfsRootDirectory;
            if(tmpHdfsHost != null && tmpHdfsHost.compareTo("") != 0)
                hdfsHost = tmpHdfsHost;
            if(tmpHdfsUsername != null && tmpHdfsUsername.compareTo("") != 0)
                hdfsUsername = tmpHdfsUsername;
            if(tmpHdfsGroupname != null && tmpHdfsGroupname.compareTo("") != 0)
                hdfsGroupname = tmpHdfsGroupname;

            if(tmpK != null && tmpK.compareTo("") != 0) {
                try {
                    ConstantsAndDefaults.QUERY_ANSWER_SIZE = Integer.
                            parseInt(tmpK);
                } catch (NumberFormatException e) {
                }
            }

            if(tmpTime != null && tmpTime.compareTo("") != 0) {
                try {
                    ConstantsAndDefaults.QUERY_TIME = Long.parseLong(tmpTime)
                            *60*1000;
                } catch (NumberFormatException e) {
                }
            }

            if(tmpMemoryIndexCapacity != null && tmpMemoryIndexCapacity
                    .compareTo("") != 0) {
                try {
                    ConstantsAndDefaults.MEMORY_INDEX_CAPACITY = Integer.
                            parseInt(tmpMemoryIndexCapacity);
                } catch (NumberFormatException e) {
                }
            }

            if(tmpMemoryIndexNumSegments != null && tmpMemoryIndexNumSegments
                    .compareTo("") != 0) {
                try {
                    ConstantsAndDefaults.MEMORY_INDEX_DEFAULT_SEGMENTS = Integer.
                            parseInt(tmpMemoryIndexNumSegments);
                } catch (NumberFormatException e) {
                }
            }

            if(tmpLogsDirectory != null && tmpLogsDirectory.compareTo("") !=
             0) {
                logsDirectory = tmpLogsDirectory;
                if(!logsDirectory.endsWith(File.separator))
                    logsDirectory += File.separator;
            }

        } catch (IOException e) {
        }
    }

    private static void loadMetadata() {
        byte [] metadataBytes = KiteUtils.readHDFSBlock(KiteInstance
                .getMetadataDirectory(), KiteInstance.getMetadataFileId());
        metadata.clear();
        if(metadataBytes != null) {
            ByteStream metadataSerializer = new ByteStream(metadataBytes);
            //load memory entries
            MetadataEntry entry = MetadataEntry.deserialize(metadataSerializer);
            while (entry != null) {
                if (entry.isStreamEntry() || entry.isIndexEntry()) {
                    metadata.add(entry);
                }
                entry = MetadataEntry.deserialize(metadataSerializer);
            }
        }
    }

    private static boolean restart(MetadataEntry metaData) {
        Pair<Pair<Boolean, String>, MQLResults> executionResults = null;
        if(metaData.isIndexEntry()) {
            IndexMetadataEntry indexMetadata = (IndexMetadataEntry)metaData;
            executionResults = MQL.executeIndexingRequest(indexMetadata, false);
            boolean successfulExecution = executionResults.getKey()
                    .getKey();
            String executionErrorMessage = executionResults.getKey()
                    .getValue();
            MQLResults results = executionResults.getValue();
            if(successfulExecution) {
                String msg = indexMetadata.getIndexName()+" " +
                        "index successfully restarted on "+indexMetadata
                        .getStreamName()+":"+indexMetadata.getAttributeName()
                        +"!";
                System.out.println(msg);
                KiteInstance.logOut(msg);
            }
            else {
                System.err.println(executionErrorMessage);
                KiteInstance.logError(executionErrorMessage);
            }
            return successfulExecution;
        } else if(metaData.isStreamEntry()) {
            StreamMetadataEntry streamMetadata = (StreamMetadataEntry)metaData;
            executionResults = MQL.executeStreamCreation(streamMetadata,false);
            boolean successfulExecution = executionResults.getKey()
                    .getKey();
            String executionErrorMessage = executionResults.getKey()
                    .getValue();
            MQLResults results = executionResults.getValue();
            if(successfulExecution) {
                String msg = "Stream "+streamMetadata.getStreamName()+" " +
                        "connection restarted successfully!";
                System.out.println(msg);
                KiteInstance.logOut(msg);
            }
            else {
                System.err.println(executionErrorMessage);
                KiteInstance.logError(executionErrorMessage);
            }
            return successfulExecution;
        }
        return false;
    }

    public static void setKite(KiteLaunchTool kite) {
        KiteInstance.kite = kite;
    }

    public static FileSystem hdfs() {
        return hdfs;
    }

    public static void setHDFS() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.default.name", hdfsHost);
            hdfs = FileSystem.get(conf);
            hdfs.setOwner(new Path(hdfsRootDirectory), hdfsUsername, hdfsGroupname);
        } catch (Exception e) {
            String errMsg = "Failed to access Hadoop Distributed File " +
                    "System (HDFS).";
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            System.err.println(errMsg);
            KiteInstance.logError(errMsg);
            KiteInstance.closeGracefully(1);
        }
    }

    public static String getHdfsRootDirectory() {
        return hdfsRootDirectory;
    }

    public static String getKiteRootDirectory() {
        return KiteInstance.getHdfsRootDirectory()+"kite/";
    }

    public static boolean isExistingStream(String streamName) {
        StreamMetadataEntry entry = getStreamMetadataEntry(streamName);
        return entry != null;
    }

    public static boolean isExistingAttribute(String streamName,
                                              String attributeName) {
        StreamMetadataEntry entry = getStreamMetadataEntry(streamName);
        if(entry != null)
            return entry.attributeExists(attributeName);
        else
            return false;
    }

    public static boolean isSpatialAttribute(String streamName,
                                              String attributeName) {
        StreamMetadataEntry entry = getStreamMetadataEntry(streamName);
        if(entry != null)
            return entry.isSpatialAttributeExists(attributeName);
        else
            return false;
    }

    private static StreamMetadataEntry getStreamMetadataEntry(String streamName)
    {
        for(MetadataEntry entry: metadata) {
            if(entry.isStreamEntry()) {
                StreamMetadataEntry streamEntry = (StreamMetadataEntry) entry;
                if(streamEntry.isMatchingName(streamName)) {
                    return streamEntry;
                }
            }
        }
        return null;
    }

    public static void addStream(String streamName, StreamDataset stream) {
        streams.put(streamName, stream);
    }

    public static void removeStream(String streamName) {
        streams.remove(streamName);
    }

    public static StreamDataset getStream(String streamName) {
        return streams.get(streamName);
    }

    public static boolean addMetadata(MetadataEntry metadataEntry) {
        //add it to memory
        metadata.add(metadataEntry);
        boolean written = writeMetadataToDisk();
        //notify other nodes in the cluster with updated metadata
        if(!written)
            metadata.remove(metadata.size()-1);
        return written;
    }

    private static String getMetadataFileId() {
        return "metadata";
    }

    public static String getMetadataDirectory() {
        return getKiteRootDirectory()+"metadata/";
    }

    public static Attribute getAttribute(String streamName,
                                         String attributeName) {
        StreamMetadataEntry entry = getStreamMetadataEntry(streamName);
        if(entry != null)
            return entry.getAttribute(attributeName);
        else
            return null;
    }

    private static IndexMetadataEntry getIndexMetadataEntry(String streamName,
                                                             String indexName) {
        for(MetadataEntry entry: metadata) {
            if(entry.isIndexEntry()) {
                IndexMetadataEntry indexEntry = (IndexMetadataEntry) entry;
                if(indexEntry.isMatching(streamName, indexName)) {
                    return indexEntry;
                }
            }
        }
        return null;
    }

    public static boolean isExistingIndex(String streamName, String indexName) {
        return getIndexMetadataEntry(streamName, indexName) != null;
    }

    public static boolean isHomeNode(String streamName) {
        return streams.get(streamName)!=null;
    }

    public static boolean restartStream(String streamName) {
        StreamMetadataEntry streamMetadata = getStreamMetadataEntry(streamName);
        if(streamMetadata != null){
            return restart(streamMetadata);
        } else return false;
    }

    public static boolean restartStreamIndexes(String streamName) {
        ArrayList<IndexMetadataEntry> indexesMetadata = getIndexMetadataEntry
                (streamName);
        boolean successfulRestart = true;
        for(IndexMetadataEntry indexMetadata: indexesMetadata) {
            successfulRestart = restart(indexMetadata);
        }
        return successfulRestart;
    }

    private static ArrayList<IndexMetadataEntry> getIndexMetadataEntry(
                                                            String streamName) {
        ArrayList<IndexMetadataEntry> indexesMetadata = new ArrayList<>();
        for(MetadataEntry metadataEntry: metadata) {
            if(metadataEntry.isIndexEntry()) {
                IndexMetadataEntry indexMetadata = (IndexMetadataEntry)
                        metadataEntry;
                if(indexMetadata.isMatchingStream(streamName))
                    indexesMetadata.add(indexMetadata);
            }
        }
        return indexesMetadata;
    }

    public static boolean removeStreamMetadata(String streamName) {
        boolean result = false;
        ArrayList<MetadataEntry> tmpRemovedList = new ArrayList<>();
        //remove stream entry
        for(int i = metadata.size()-1; i >= 0; --i) {
            if(metadata.get(i).isStreamEntry()) {
                StreamMetadataEntry streamEntry = (StreamMetadataEntry)
                        metadata.get(i);
                if(streamEntry.isMatchingName(streamName)) {
                    tmpRemovedList.add(metadata.get(i));
                    metadata.remove(i);
                    result = true;
                }
            }
        }

        //remove stream indexes
        if(result) {
            for(int i = metadata.size()-1; i >= 0; --i) {
                if(metadata.get(i).isIndexEntry()) {
                    IndexMetadataEntry indexEntry = (IndexMetadataEntry)
                            metadata.get(i);
                    if(indexEntry.isMatchingStream(streamName)) {
                        tmpRemovedList.add(metadata.get(i));
                        metadata.remove(i);
                    }
                }
            }
            result = result && writeMetadataToDisk();
        }
        if(!result)
            metadata.addAll(tmpRemovedList);
        else
            tmpRemovedList.clear();
        return result;
    }

    private static boolean writeMetadataToDisk() {
        int estimateSize = metadata.size()*300;
        ByteStream metadataSerializer = new ByteStream(estimateSize);
        for(MetadataEntry entry: metadata)
            entry.serialize(metadataSerializer);

        try {
            KiteUtils.writeHDFSBlock(metadataSerializer, KiteInstance
                    .getMetadataDirectory(), KiteInstance.getMetadataFileId());
            if(getNodesCount() > 1)
                getCluster().compute(getCluster().cluster().forRemotes())
                        .broadcast(() -> KiteInstance.loadMetadata());
            return true;
        } catch (IOException e) {
            String errMsg = "Unable to write metadata to HDFS.";
            errMsg += System.lineSeparator();
            errMsg += "Error: "+e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            return false;
        }
    }

    public static boolean removeIndexMetadata(String streamName, String
            indexName) {
        MetadataEntry removedEntry = null;
        boolean result = false;
        for(int i = 0; i < metadata.size(); ++i) {
            if(metadata.get(i).isIndexEntry()) {
                IndexMetadataEntry indexEntry = (IndexMetadataEntry)
                        metadata.get(i);
                if(indexEntry.isMatching(streamName, indexName)) {
                    removedEntry = metadata.get(i);
                    metadata.remove(i);
                    result = writeMetadataToDisk();
                    if(!result)
                        metadata.add(removedEntry);
                    return result;
                }
            }
        }
        return false;
    }

    public static void descStream(String streamName) {
        StreamMetadataEntry streamMetadata = getStreamMetadataEntry(streamName);
        desc(streamMetadata);
        if(streamMetadata != null) {
            ArrayList<IndexMetadataEntry> indexesMetadata =
                                            getIndexMetadataEntry(streamName);
            for(IndexMetadataEntry entry: indexesMetadata)
                desc(entry);
        }
    }

    private static void desc(MetadataEntry entry) {
        if(entry != null) {
            String entryStr = entry.toString();
            System.out.println(entryStr);
            KiteInstance.logOut(entryStr);
        }
    }

    public static void descAllStreams() {
        for(MetadataEntry entry: metadata)
            desc(entry);
    }

    public static int getNodesCount() {
        long topVer = getCluster().cluster().topologyVersion();
        return kite.getCluster().cluster().topology(topVer).size();
    }

    public static String getNowTimestamp() {
        String timestamp = Instant.now().toString();
        timestamp = timestamp.replaceAll(":","-");
        timestamp = timestamp.replaceAll("/","-");
        return timestamp;
    }

    public static void pauseAllActiveStreams() {
        for(StreamDataset stream: streams.values()) {
            stream.stop();
        }
        //wait until on-going data insertion is done
        for(StreamDataset stream: streams.values()) {
            while (stream.inInsertion())
                KiteUtils.busyWait(50);
        }
    }

    public static void closeLogs() {
        try {
            if(outLog != null) {
                outLog.flush();
                outLog.close();
            }
            if(errorLog != null) {
                errorLog.flush();
                errorLog.close();
            }
            if(showLog != null) {
                showLog.flush();
                showLog.close();
            }
        } catch (IOException e) {
        }
    }

    public static void closeGracefully(int exitStatus) {
        KiteInstance.pauseAllActiveStreams();
        String msg = "All active streams are paused, Kite node exits!!";
        if(exitStatus == 0) {
            KiteInstance.logOut(msg);
            System.out.println(msg);
        } else {
            KiteInstance.logError(msg);
            System.err.println(msg);
        }
        KiteInstance.closeLogs();
        KiteInstance.printMessageOnAllClusterNodes("Node "+KiteInstance
                .getLocalNodeName()+" exits!");
        System.exit(exitStatus);
    }

    public static void printMessageOnAllClusterNodes(String msg) {
        getCluster().compute().broadcast(() -> System.out.println(msg));
    }

    public static String getLocalNodeName() {
        return KiteInstance.getCluster().cluster().localNode().hostNames().
                iterator().next();
    }
}
