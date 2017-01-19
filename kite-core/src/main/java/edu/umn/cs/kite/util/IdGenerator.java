package edu.umn.cs.kite.util;

/**
 * Created by amr_000 on 10/19/2016.
 */
public class IdGenerator {

    private long id = 0;
    public IdGenerator(){}
    public IdGenerator(long initId){id = initId;}
    public long nextId(){return id++;}
}
