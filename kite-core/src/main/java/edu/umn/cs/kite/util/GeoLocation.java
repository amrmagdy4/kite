package edu.umn.cs.kite.util;

import edu.umn.cs.kite.util.serialization.ByteStream;

/**
 * Created by amr_000 on 8/1/2016.
 */
public interface GeoLocation {
    int serialize(ByteStream byteStream);

    static GeoLocation deserialize(ByteStream byteStream) {
        byte locationType = byteStream.readByte();
        GeoLocation location;
        switch (locationType){
            case 0:
                location = null;
                break;
            case 1://PointLocation
                double latitude = byteStream.readDouble();
                double longitude = byteStream.readDouble();
                location = new PointLocation(latitude, longitude);
                break;
            case 2://Rectangle
                double north = byteStream.readDouble();
                double south = byteStream.readDouble();
                double east = byteStream.readDouble();
                double west = byteStream.readDouble();
                location = new Rectangle(north, south, east, west);
                break;
            default:
                location = null;
                break;
        }
        return location;
    }

    boolean overlap(GeoLocation rect);

    boolean isPoint();

    static int serializeNull(ByteStream byteStream) {
        byte type = 0;
        byteStream.write(type);
        return Byte.BYTES;
    }
}
