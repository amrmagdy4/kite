package edu.umn.cs.kite.util;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.querying.MQL;
import edu.umn.cs.kite.querying.metadata.MetadataEntry;
import javafx.util.Pair;

import java.nio.channels.InterruptedByTimeoutException;

/**
 * Created by amr_000 on 1/18/2017.
 */
public class TimerExecution implements Runnable {

    private boolean doneExecution = false;
    private Pair<Pair<Boolean, String>, MetadataEntry> parsingResults = null;

    public boolean executeQuery(Pair<Pair<Boolean, String>, MetadataEntry>
                                     parsingResults, long durationMilliseconds){
        this.parsingResults = parsingResults;
        Thread executionThread = new Thread(this);
        executionThread.start();
        boolean done = waitExecution(durationMilliseconds);
        if(!done) {
            executionThread.interrupt();
        }
        return done;
    }

    private boolean waitExecution(long durationMilliseconds) {

        long startTime = System.currentTimeMillis();
        long currTime = System.currentTimeMillis();
        long duration = currTime-startTime;
        int waitTime = 1;
        while(!doneExecution && duration < durationMilliseconds) {
            KiteUtils.busyWait(waitTime);
            if(waitTime < 50)
                waitTime *= 2;
            else waitTime = 50;
            currTime = System.currentTimeMillis();
            duration = currTime-startTime;
        }
        return doneExecution;
    }

    @Override
    public void run() {
        try {
            doneExecution = false;
            if (parsingResults != null) {
                MQL.executeStatement(parsingResults);
            }
            doneExecution = true;
        } catch (Exception e) {
        }
    }
}
