package edu.umn.cs.kite.util.serialization;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by amr_000 on 8/25/2016.
 */
public class ByteStream {

    private final int DELTA = 1024;
    private int capacity ;
    private ByteBuffer byteStream ;

    public ByteStream(int capacity) {
        this.capacity = 2*capacity+4*DELTA;
        byteStream = ByteBuffer.allocate(this.capacity);
    }

    public ByteStream(byte [] bytes) {
        byteStream = ByteBuffer.wrap(bytes);
    }

    public int write(byte[] bytes) {
        byteStream.put(bytes);
        return Byte.BYTES*bytes.length;
    }

    public int write(byte[] bytes, int length) {
        byteStream.put(bytes,0,length);
        return Byte.BYTES*length;
    }

    public int write(byte oneByte) {
        byteStream.put(oneByte);
        return Byte.BYTES;
    }

    public int write(int value) {
        byteStream.putInt(value);
        return Integer.BYTES;
    }

    public int write(long value) {
        byteStream.putLong(value);
        return Long.BYTES;
    }

    public int write(String str) {
        int bytesLen = 0;
        if(str == null) {
            bytesLen += this.write(0);
        }
        else {
            byte[] strBytes = str.getBytes();

            bytesLen += this.write(strBytes.length); //key length
            bytesLen += this.write(strBytes);
        }
        return bytesLen;
    }

    public byte[] getBytes() { return byteStream.array(); }

    public int getWrittenBytes() { return byteStream.position(); }

    public int getReadBytes() { return byteStream.position(); }

    public boolean hasRemaining(){return byteStream.hasRemaining();}

    public int write(double dblVal) {
        byteStream.putDouble(dblVal);
        return Double.BYTES;
    }

    public int readInt() {
        return byteStream.getInt();
    }

    public byte[] readBytes(int bytesLen) {
        byte [] bytes = new byte[bytesLen];
        byteStream.get(bytes,0,bytesLen);
        return bytes;
    }

    public boolean skipBytes(int bytesLen) {
        try {
            byteStream.position(byteStream.position()+bytesLen);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public long readLong() {
        return byteStream.getLong();
    }

    public String readString() {
        int keyBytesLen = readInt();
        if(keyBytesLen == 0)
            return null;
        byte [] keyStrBytes = readBytes(keyBytesLen);
        return new String(keyStrBytes);
    }

    public byte readByte() {
        byte [] bytes = readBytes(1);
        return bytes[0];
    }

    public double readDouble() {
        return byteStream.getDouble();
    }

    public void clear() {
        byteStream.clear();
    }

    public byte[] getBytesClone() {
        int lenToCopy = getWrittenBytes();
        return Arrays.copyOfRange(byteStream.array(),0, lenToCopy);
    }

    public int getCapacity() {
        return capacity;
    }
}
