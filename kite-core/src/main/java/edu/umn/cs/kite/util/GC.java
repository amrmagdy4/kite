package edu.umn.cs.kite.util;

/**
 * Created by amr_000 on 1/12/2017.
 */
public class GC implements Runnable {
    public static void invoke() {
        Thread gcThread = new Thread(new GC());
        gcThread.start();
    }

    @Override
    public void run() {
        System.gc();
    }
}
