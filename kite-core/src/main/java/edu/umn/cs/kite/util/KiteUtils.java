package edu.umn.cs.kite.util;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.util.microblogs.Microblog;
import edu.umn.cs.kite.util.serialization.ByteStream;
import javafx.util.Pair;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by amr_000 on 8/4/2016.
 */
public class KiteUtils {
    public static double GreatCircleDistanceMiles(double lat1, double lat2,
                                                  double lng1, double lng2) {
        //approximate value
        return Math.sqrt(Math.pow(69.1*(lat1-lat2),2)+Math.pow(53*(lng1-lng2),
                2));
    }


    public static String int2str(int counter, int n) {
        String counterStr = Integer.toString(counter);
        while (counterStr.length() < n)
            counterStr = "0"+counterStr;
        return counterStr;
    }

    public static int binarySearch (Microblog[] microblogs, int low, int high,
            Timestamp timestamp) {
        //locate the latest microblog with time <= timestamp
        if(microblogs[high].getTimestamp() < timestamp.getTime())
            return high;
        if(microblogs[low].getTimestamp() > timestamp.getTime())
            return -1;

        double epsilon = 1.1;
        int l = low, r = high;
        while (l < r) {
            int m = (int)Math.floor(1.0*(l+r)/2.0);
            if(Math.abs(microblogs[m].getTimestamp()-timestamp.getTime()) <=
                    epsilon) {
                //approximate match, get the exact match
                if (microblogs[m].getTimestamp() <= timestamp.getTime()) {
                    while(microblogs[m].getTimestamp() <= timestamp.getTime())
                        ++m;
                    return m-1;
                }
                else {
                    while(microblogs[m].getTimestamp() > timestamp.getTime())
                        --m;
                    return m;
                }
            }
            else {
                if (microblogs[m].getTimestamp() < timestamp.getTime())
                    l = m+1;
                else if(microblogs[m].getTimestamp() > timestamp.getTime())
                    r = m-1;
            }
        }
        return -1;
    }

    public static int binarySearch_StartsWithin(List<TemporalPeriod>
                                                        timePeriodsList,
                                                Timestamp timestamp) {
        int l = 0, r = timePeriodsList.size();
        while (l < r) {
            int m = (int)Math.floor(1.0*(l+r)/2.0);
            if(timePeriodsList.get(m).overlap(timestamp))
                return m;
            else {
                if (timePeriodsList.get(m).endsBefore(timestamp))
                    l = m+1;
                else //(timePeriodsList.get(m).startsAfter(timestamp))
                            r = m-1;
            }
        }
        return -1;
    }

    public static int binarySearch_StartOverlap(List<TemporalPeriod>
                                                        timePeriodsList,
                                                TemporalPeriod period) {
        int ind = binarySearch_StartsWithin(timePeriodsList, period.from());
        if(ind < 0) {
            if(timePeriodsList != null && timePeriodsList.size() > 0) {
                if (period.overlap(timePeriodsList.get(0).from()))
                    return 0;
                else
                    return -1;
            } else
                return -1;
        } else
            return ind;
    }

    public static int binarySearch_StartOverlap2(List<TemporalPeriod>
                                                    timePeriodsList,
                                                TemporalPeriod period) {
        //check old periods that starts before the time horizon
        if(period.startsBefore(timePeriodsList.get(0)) &&
                !period.endsBefore(timePeriodsList.get(0)))
            return 0;

        //if not old, search within the time horizon
        int l = 0, r = timePeriodsList.size();
        while (l < r) {
            int m = (int)Math.floor(1.0*(l+r)/2.0);
            if(period.startsWithin(timePeriodsList.get(m)))
                return m;
            else {
                if (period.startsAfter(timePeriodsList.get(m)))
                    l = m+1;
                else if(period.startsBefore(timePeriodsList.get(m)))
                    r = m-1;
            }
        }
        return -1;
    }

    public static int binarySearch_EndOverlap(List<TemporalPeriod>
                                                        timePeriodsList,
                                                TemporalPeriod period) {
        int ind = binarySearch_StartsWithin(timePeriodsList, period.to());
        if(ind < 0){
            if(timePeriodsList != null && timePeriodsList.size() > 0) {
                if (period.overlap(timePeriodsList.get(timePeriodsList.size()
                        -1).to()))
                    return timePeriodsList.size()-1;
                else
                    return -1;
            } else
                return -1;
        } else
            return ind;
    }

    public static long getFileCount(String hdfsDirectory) {
        Path path = new Path(hdfsDirectory);
        FileSystem hdfs = KiteInstance.hdfs();
        long fileCount = 0;
        try {
            if(hdfs.exists(path)){
                ContentSummary cs = hdfs.getContentSummary(path);
                fileCount = cs.getFileCount();
            }
        } catch (IOException e) {
        }
        return fileCount;
    }
    public static byte[] readHDFSBlock(String parentDirectory, String fileId) {
        String hdfsPath = parentDirectory+fileId;

        Hashtable<String,ArrayList<Microblog>> blockEntries = new Hashtable<>();
        FileSystem hdfs = KiteInstance.hdfs();
        try {
            Path path = new Path(hdfsPath);

            long fileLenBytes = hdfs.getFileStatus(path).getLen();
            FSDataInputStream block = hdfs.open(path,
                    ConstantsAndDefaults.BUFFER_SIZE_BYTES);
            byte [] fileBytes = new byte[(int)fileLenBytes];
            block.readFully(0, fileBytes, 0, (int)fileLenBytes);
            return fileBytes;
        } catch (IOException e) {
            return null;
        }
    }

    public static String writeHDFSBlock(ByteStream serializedBytes, String
            parentDirectory, String fileId) throws
            IOException {
        byte [] bytes = serializedBytes.getBytes();
        int blockSize = Math.max(1024*1024, (int)(Math.ceil(serializedBytes
                .getWrittenBytes()/512.0)*512));

        FileSystem hdfs = KiteInstance.hdfs();

        Path fileP = new Path(parentDirectory, fileId);
        hdfs.mkdirs(fileP.getParent());

        FSDataOutputStream file = hdfs.create(fileP, true, ConstantsAndDefaults
                .BUFFER_SIZE_BYTES, (short)1, blockSize);
        file.write(bytes, 0, serializedBytes.getWrittenBytes());
        file.flush();
        file.close();

        return fileId;
    }

    public static ByteStream checkByteStreamCapacity(ByteStream bstream, int
            delta) {
        if(bstream.getWrittenBytes()+delta >= bstream.getCapacity()) {
            //if no space, enlarge the serializer
            int copyLength = bstream.getWrittenBytes();
            byte [] copyBytes = Arrays.copyOfRange(bstream.getBytes(), 0,
                    copyLength);
            int newLen = Math.max(bstream.getCapacity()+delta,
                                    bstream.getCapacity()*2);
            bstream = new ByteStream(newLen);
            bstream.write(copyBytes);
        }
        return bstream;
    }

    public static void busyWait(int durationMilliseconds) {
        long startTime = System.currentTimeMillis();
        int duration = 0;
        do {
            duration = (int) (System.currentTimeMillis()-startTime);
        }while (duration < durationMilliseconds);
    }

    public static Pair<Integer,Long> loadStreamIds(Pair<String, String>
                                                           dirFileidPair,
                                                   Scheme microblogScheme) {
        String parentDir = dirFileidPair.getKey();
        String fileIdPrefix = dirFileidPair.getValue();

        int recoveryBlockCount = (int) getFileCount(parentDir);
        long latestId = -1;
        if(recoveryBlockCount > 0) {
            String lastRecoveryFileId = fileIdPrefix+getRecoveryFileId
                    (recoveryBlockCount-1);
            byte [] lastBlockBytes = readHDFSBlock(parentDir,
                    lastRecoveryFileId);
            ByteStream byteStream = new ByteStream(lastBlockBytes);
            Microblog microblog, lastMicroblog = null;
            while ((microblog = Microblog.deserialize(byteStream,
                    microblogScheme))!= null) {
                lastMicroblog = microblog;
            }
            latestId = lastMicroblog.getId();
        }
        return new Pair<>(recoveryBlockCount, latestId);
    }

    public static String getRecoveryFileId(int cntr) {
        return KiteUtils.int2str(cntr, 6);
    }
}
