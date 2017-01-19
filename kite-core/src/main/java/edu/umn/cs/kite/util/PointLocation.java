package edu.umn.cs.kite.util;

import edu.umn.cs.kite.util.serialization.ByteStream;

/**
 * Created by amr_000 on 8/4/2016.
 */
public class PointLocation implements GeoLocation {

    private double latitude, longitude;

    public PointLocation (double lat, double lng) {
        latitude = lat;
        longitude = lng;

    }

    public PointLocation (double [] coords) {
        latitude = coords[0];
        longitude = coords[1];
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    @Override
    public int serialize(ByteStream byteStream) {
        int byteLen = 0;

        byte locationType = 1;
        byteLen += byteStream.write(locationType);
        byteLen += byteStream.write(latitude);
        byteLen += byteStream.write(longitude);

        return byteLen;
    }

    @Override
    public boolean overlap(GeoLocation loc) {
        if(loc.isPoint()) {
            return this.equals((PointLocation)loc);
        } else {
            Rectangle rect = (Rectangle) loc;
            return rect.isInside(this);
        }
    }

    @Override public boolean isPoint() {return true;}

    public String toString() { return "("+latitude+","+longitude+")"; }

    @Override public boolean equals(Object other) {
        double epsilon = 0.000001;
        PointLocation pt = (PointLocation) other;
        return Math.abs(getLatitude()-pt.getLatitude()) < epsilon &&
                Math.abs(getLongitude()-pt.getLongitude()) < epsilon;
    }

}
