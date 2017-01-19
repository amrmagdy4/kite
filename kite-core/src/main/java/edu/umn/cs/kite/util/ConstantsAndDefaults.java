package edu.umn.cs.kite.util;

import java.util.Date;

/**
 * Created by amr_000 on 9/15/2016.
 */
public class ConstantsAndDefaults {
    public static long GC_PERIOD_MILLISECONDS = 15*60*1000;
    public static long LOG_PERIOD_MILLISECONDS = 2*60*1000;
    public static int FILE_BLK_SIZE_BYTES = 128*1024*1024*1;
    public static int BUFFER_SIZE_BYTES = 4096 * 1024;
    public static int RECOVERY_BLK_SIZE_BYTES = 64*1024*1024*1;

    //Memory data constants
    public final static int MAX_MEMORY_CHUNK_CAPACITY = 3000000;
    public final static double MEMORY_DATA_FLUSH_THRESHOLD = 0.2;

    //Memory indexes
    public static int MEMORY_INDEX_DEFAULT_SEGMENTS = 5;
    public static final long BATCH_UPDATE_TIME_MILLISECONDS = 3000;
    public static final int TEMPORAL_FLUSHING_PERIODIC_TIME_MINUTES = 360;
    public static final int TEMPORAL_FLUSHING_PERIODIC_DATA_SIZE = 1000000;

    public static int MEMORY_INDEX_CAPACITY =
            TEMPORAL_FLUSHING_PERIODIC_DATA_SIZE;

    //Disk indexes
    public static String DISK_INDEXES_ROOT_DIRECTORY;
    public static final int NUM_DISK_SEGMENTS_IN_MEMORY_DIRECTORY = 10;

    //Query default values
    public static int QUERY_ANSWER_SIZE = 20;
    public static long QUERY_TIME = 3*60*60*1000;//3 hours
    public static TemporalPeriod QUERY_TIME() {
        long to = new Date().getTime();
        long from = to - QUERY_TIME;
        return new TemporalPeriod(from, to);
    }
}
