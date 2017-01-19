package edu.umn.cs.kite.util;

import edu.umn.cs.kite.util.serialization.ByteStream;

/**
 * Created by amr_000 on 8/4/2016.
 */
public class Rectangle implements GeoLocation {
    private double north, south, east, west;
    private double widthMiles, heightMiles;

    public Rectangle (double north, double south, double east, double west) {
        this.north = north;
        this.south = south;
        this.east = east;
        this.west = west;
        widthMiles = KiteUtils.GreatCircleDistanceMiles(north,north,west,east);
        heightMiles = KiteUtils.GreatCircleDistanceMiles(north,south,west,west);
    }

    public Rectangle(PointLocation center, double latWidth, double lngWidth) {
        this(center.getLatitude()+latWidth, center.getLatitude()-latWidth,
                center.getLatitude()+lngWidth, center.getLatitude()-lngWidth);
    }

    public double getWidth() {
        return widthMiles;
    }

    public double getHeight() {
        return heightMiles;
    }

    public boolean isInside(GeoLocation location) {
        if (location instanceof PointLocation)
            return isInside((PointLocation)location);
        else if (location instanceof Rectangle)
            return isInside((Rectangle)location);
        else throw new IllegalArgumentException("Niether point nor " +
                    "rectanlgular location type");
    }
    public boolean isInside(PointLocation point) {
        if(point.getLatitude() >= south && point.getLatitude() <= north
                && point.getLongitude() >= west && point.getLongitude() <= east)
            return true;
        else
            return false;
    }

    public boolean isInside(Rectangle other) {
        if(other.getSouth() >= south && other.getNorth() <= north
                && other.getWest() >= west && other.getEast() <= east)
            return true;
        else
            return false;
    }

    public double getNorth() {
        return north;
    }

    public double getSouth() {
        return south;
    }

    public double getEast() {
        return east;
    }

    public double getWest() {
        return west;
    }

    public boolean isValid() {
        return south <= north && west <= east;
    }

    public Rectangle merge(Rectangle other) {
        if(this.equals(other))
            return other;
        Rectangle mergedRect = new Rectangle(Math.max(north,other.getNorth()),
                Math.min(south,other.getSouth()), Math.max(east,other.getEast
                ()), Math.min(west, other.getWest()));
        return mergedRect;
    }

    @Override
    public int serialize(ByteStream byteStream) {
        int byteLen = 0;

        byte locationType = 2;
        byteLen += byteStream.write(locationType);
        byteLen += byteStream.write(north);
        byteLen += byteStream.write(south);
        byteLen += byteStream.write(east);
        byteLen += byteStream.write(west);

        return byteLen;
    }

    @Override
    public boolean overlap(GeoLocation loc) {
        if(loc.isPoint()) {
            PointLocation pt = (PointLocation)loc;
            return this.isInside(pt);
        } else {
            Rectangle rect = (Rectangle) loc;
            return isInside(rect.getNW()) || isInside(rect.getNE()) ||
                    isInside(rect.getSW()) || isInside(rect.getSE());
        }
    }

    @Override public boolean isPoint() {return false;}

    private PointLocation getNW() {
        return new PointLocation(this.getNorth(),this.getWest());
    }

    private PointLocation getNE() {
        return new PointLocation(this.getNorth(),this.getEast());
    }

    private PointLocation getSW() {
        return new PointLocation(this.getSouth(),this.getWest());
    }

    private PointLocation getSE() {
        return new PointLocation(this.getSouth(),this.getEast());
    }

    public String toString() { return "("+north+","+south+","+east+"," +
            ""+west+")"; }

    public static int numBytes() {
        return Byte.BYTES+4*Double.BYTES;
    }

    public boolean isBoundedCoordinates() {
        return -90 <= north && north <=90 && -90<= south && south <=90 &&
                -180<= east && east <=180 && -180<= west && west <=180;
    }

    @Override public boolean equals(Object other) {
        double epsilon = 0.000001;
        Rectangle pt = (Rectangle) other;
        return Math.abs(getNorth()-pt.getNorth()) < epsilon &&
                Math.abs(getSouth()-pt.getSouth()) < epsilon &&
                Math.abs(getEast()-pt.getEast()) < epsilon &&
                Math.abs(getWest()-pt.getWest()) < epsilon;
    }
}
