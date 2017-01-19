package edu.umn.cs.kite.util;

import edu.umn.cs.kite.util.serialization.ByteStream;

import java.sql.Timestamp;

/**
 * Created by amr_000 on 9/8/2016.
 */
public class TemporalPeriod {
    public Timestamp from() {
        return from;
    }

    public Timestamp to() {
        return to;
    }

    private Timestamp from, to;

    @Override public String toString(){
        return "["+from+","+to+"]";
    }

    public TemporalPeriod(long fromTimestamp, long toTimestamp) {
        this(new Timestamp(fromTimestamp), new Timestamp(toTimestamp));
    }

    public TemporalPeriod(Timestamp fromTimestamp, Timestamp toTimestamp) {
        if(fromTimestamp != null && toTimestamp != null &&
            fromTimestamp.getTime() > toTimestamp.getTime())
            throw new IllegalArgumentException("TemporalPeriod start time is " +
                    "after its end");

        from = fromTimestamp;
        to = toTimestamp;
    }

    public boolean overlap(TemporalPeriod other) {
        return this.startsWithin(other) || other.startsWithin(this);
    }

    public boolean startsWithin(TemporalPeriod other) {
        if(this.from() == null)
            return true;
        if(other.from() == null)
            return false;
        return (other.from().getTime() >= this.from().getTime())
         && (this.to()==null || other.from().getTime() <= this.to().getTime());
    }

    public boolean startsAfter(TemporalPeriod other) {
        return this.from().getTime() > other.to().getTime();
    }

    public boolean startsBefore(TemporalPeriod other) {
        return this.from().getTime() < other.from().getTime();
    }

    public boolean endsBefore(TemporalPeriod other) {
        return this.to().getTime() < other.from().getTime();
    }

    public void setFrom(long from) {
        this.from = new Timestamp(from);
    }

    public void setTo(long to) {
        this.to = new Timestamp(to);
    }

    public boolean overlap(Timestamp timestamp) {
        return (from()==null || timestamp.getTime() >= this.from().getTime()) &&
        (to()==null || timestamp.getTime() <= this.to().getTime());
    }

    public boolean overlap(long timestamp) {
        return timestamp >= this.from().getTime() &&
                timestamp <= this.to().getTime();
    }

    public boolean endsBefore(Timestamp timestamp) {

        if(timestamp == null) {
            if(this.to() != null)
                return true;
            else return false;
        }
        if(this.to() == null)
            return false;
        return this.to().getTime() < timestamp.getTime();
    }

    public boolean startsAfter(Timestamp timestamp) {
        return this.from().getTime() > timestamp.getTime();
    }

    public void serialize(ByteStream byteStream) {
        byteStream.write(this.from().getTime());
        byteStream.write(this.to().getTime());
    }

    public static TemporalPeriod deserialize(ByteStream byteStream) {
        if(!byteStream.hasRemaining())
            return null;
        long from = byteStream.readLong();
        long to = byteStream.readLong();
        return new TemporalPeriod(from, to);
    }
}
