package edu.umn.cs.kite.datamodel;

import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.PointLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.serialization.ByteStream;
import edu.umn.cs.kite.util.serialization.Serializer;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by amr_000 on 9/2/2016.
 */
public class Attribute {
    private String name;
    private String type;

    private static Hashtable<String, Class> dataTypes;
    static {
        dataTypes = new Hashtable<>();
        dataTypes.put("long", Long.class);
        dataTypes.put("string", String.class);
        dataTypes.put("geolocation", GeoLocation.class);
        dataTypes.put("timestamp", Date.class);

        dataTypes.put("list<long>", List.class);
        dataTypes.put("list<string>", List.class);
        dataTypes.put("list<leolocation>", List.class);
        dataTypes.put("list<timestamp>", List.class);
    }

    public Attribute(String name, String type) {
        this.name = name.toLowerCase();
        this.type = type.toLowerCase();
    }

    @Override public String toString(){return "<"+name+":"+type+">";}
    @Override public boolean equals(Object other) {
        Attribute otherAttribute = (Attribute)other;
        return this.getName().compareTo(otherAttribute.getName()) == 0 &&
               this.getType().compareTo(otherAttribute.getType()) == 0;
    }
    public String getName() {return name;}

    public String getType() {return type;}

    public boolean isSpatialAttribute(){
        return getType().compareTo("geolocation")==0 ||
                getType().compareTo("list<geolocation>")==0;
    }

    public static boolean isValidDataType(String attributeType) {
        return dataTypes.containsKey(attributeType);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public boolean isList(){return type.startsWith("list");}

    public Object parseValue(String valStr) {
        Object val = null;
        switch (type){
            case "string": case "list<string>":
                val = valStr.toLowerCase();
                break;
            case "long": case "list<long>":
            case "timestamp": case "list<timestamp>":
                try {
                    val = Long.parseLong(valStr);
                } catch (NumberFormatException e) {
                    val = null;
                }
                break;
            case "geolocation": case "list<geolocation>":
                GeoLocation loc = null;
                String [] parts = valStr.split(":");
                String locType = parts[0];

                try {
                    if (locType.compareTo("null") == 0)
                        loc = null;
                    else if (locType.compareTo("2") == 0) {// 2 coordinates(point)
                        loc = new PointLocation(Double.parseDouble(parts[1]),
                                Double.parseDouble(parts[2]));
                    } else if (locType.compareTo("4") == 0) {// 4 coordinates(rectangle)
                        loc = new Rectangle(Double.parseDouble(parts[1]),
                                Double.parseDouble(parts[2]), Double.parseDouble
                                (parts[3]), Double.parseDouble(parts[4]));
                    } else
                        loc = null;
                } catch (NumberFormatException e) {
                }
                val = loc;
                break;
            default:
                val = null;
        }
        return val;
    }

    public void serializeVal(Object val, ByteStream byteStream) {
        switch (type) {
            case "string":
                String valStr = (String)val;
                byteStream.write(valStr);
                break;
            case "long":
                Long valLng = (Long)val;
                byteStream.write(valLng);
                break;
            case "timestamp":
                Long valTs = (Long)val;
                byteStream.write(valTs);
                break;
            case "geolocation":
                GeoLocation valGeo = (GeoLocation)val;
                Serializer.serializeGeoLocation(valGeo, byteStream);
                break;
            case "list<string>":
                List<String> valStrLst = (List<String>)val;
                Serializer.serializeStringList(valStrLst, byteStream);
                break;
            case "list<long>":
                List<Long> valLngLst = (List<Long>)val;
                Serializer.serializeLongList(valLngLst, byteStream);
                break;
            case "list<timestamp>":
                valLngLst = (List<Long>)val;
                Serializer.serializeLongList(valLngLst, byteStream);
                break;
            case "list<geolocation>":
                List<GeoLocation> valGeoLst = (List<GeoLocation>)val;
                Serializer.serializeGeoLocationList(valGeoLst, byteStream);
                break;
        }
    }

    public Object deserializeVal(ByteStream byteStream) {
        Object val = null;
        switch (type) {
            case "string":
                String valStr = byteStream.readString();
                val = valStr;
                break;
            case "long":
                Long valLng = byteStream.readLong();
                val = valLng;
                break;
            case "timestamp":
                Long valTs = byteStream.readLong();
                val = valTs;
                break;
            case "geolocation":
                GeoLocation valGeo = GeoLocation.deserialize(byteStream);
                val = valGeo;
                break;
            case "list<string>":
                List<String> valStrLst = Serializer.deserializeStringList
                        (byteStream);
                val = valStrLst;
                break;
            case "list<long>":
                List<Long> valLngLst = Serializer.deserializeLongList
                        (byteStream);
                val = valLngLst;
                break;
            case "list<timestamp>":
                valLngLst = Serializer.deserializeLongList
                        (byteStream);
                val = valLngLst;
                break;
            case "list<geolocation>":
                List<GeoLocation> valGeoLst = Serializer
                        .deserializeGeoLocationList (byteStream);
                val = valGeoLst;
                break;
            default:
                val = null;
        }
        return val;
    }

    public List<String> getStringListValue(Object value) {
        ArrayList<String> strLstVal = new ArrayList<>();
        if(value == null)
            return strLstVal;
        switch (type) {
            case "string":
                String valStr = (String)value;
                strLstVal.add(valStr);
                break;
            case "long":
                Long valLng = (Long)value;
                strLstVal.add(valLng.toString());
                break;
            case "timestamp":
                Long valTs = (Long)value;
                strLstVal.add(valTs.toString());
                break;
            case "geolocation":
                GeoLocation valGeo = (GeoLocation)value;
                strLstVal.add(valGeo.toString());
                break;
            case "list<string>":
                List<String> strLst = (List<String>)value;
                strLstVal.addAll(strLst);
                break;
            case "list<long>":
                List<Long> valLngLst = (List<Long>)value;
                for(int i = 0; i < valLngLst.size(); ++i) {
                    strLstVal.add(valLngLst.get(i).toString());
                }
                break;
            case "list<timestamp>":
                valLngLst = (List<Long>)value;
                for(int i = 0; i < valLngLst.size(); ++i) {
                    strLstVal.add(valLngLst.get(i).toString());
                }
                break;
            case "list<geolocation>":
                List<GeoLocation> valGeoLst = (List<GeoLocation>)value;
                for(int i = 0; i < valGeoLst.size(); ++i) {
                    strLstVal.add(valGeoLst.get(i).toString());
                }
                break;
            default:
                strLstVal = null;
        }
        return strLstVal;
    }

    public List<GeoLocation> getGeoListValue(Object value) {
        List<GeoLocation> list = new ArrayList<>();
        if(isSpatialAttribute()) {
            switch (type) {
                case "geolocation":
                    GeoLocation valGeo = (GeoLocation)value;
                    list.add(valGeo);
                    break;
                case "list<geolocation>":
                    list = (List<GeoLocation>)value;
                    break;
                default:
                    list = null;
            }
        }
        return list;
    }

    public void serialize(ByteStream byteStream) {
        byteStream.write(name);
        byteStream.write(type);
    }

    public boolean equals(Object val1, Object val2) {
        boolean result;
        switch (type) {
            case "string":
                result = ((String)val1).equals((String)val2);
                break;
            case "long":
            case "timestamp":
                result = ((Long)val1) == ((Long)val2);
                break;
            case "geolocation":
                result = ((GeoLocation)val1).equals((GeoLocation)val2);
                break;
            case "list<string>":
                List<String> strLst1 = (List<String>)val1;
                List<String> strLst2 = (List<String>)val2;
                result = true;
                if(strLst1.size() == strLst2.size()){
                    for (int i = 0; i < strLst1.size(); ++i) {
                        result = strLst1.get(i).equals(strLst2.get(i));
                    }
                }
                else result = false;
                break;
            case "list<timestamp>":
            case "list<long>":
                List<Long> lngLst1 = (List<Long>)val1;
                List<Long> lngLst2 = (List<Long>)val2;
                result = true;
                if(lngLst1.size() == lngLst2.size()){
                    for (int i = 0; i < lngLst1.size(); ++i) {
                        result = lngLst1.get(i) == lngLst2.get(i);
                    }
                }
                else result = false;
                break;
            case "list<geolocation>":
                List<GeoLocation> geoLst1 = (List<GeoLocation>)val1;
                List<GeoLocation> geoLst2 = (List<GeoLocation>)val2;
                result = true;
                if(geoLst1.size() == geoLst2.size()){
                    for (int i = 0; i < geoLst1.size(); ++i) {
                        result = geoLst1.get(i).equals(geoLst2.get(i));
                    }
                }
                else result = false;
                break;
            default:
                result = false;
        }
        return result;
    }

    public boolean overlap(Object val1, Object val2) {
        boolean result;
        switch (type) {
            case "geolocation":
                result = ((GeoLocation)val1).overlap((GeoLocation)val2);
                break;
            case "list<geolocation>":
                List<GeoLocation> geoLst1 = (List<GeoLocation>)val1;
                List<GeoLocation> geoLst2 = (List<GeoLocation>)val2;
                result = true;
                if(geoLst1.size() == geoLst2.size()){
                    for (int i = 0; i < geoLst1.size(); ++i) {
                        result = geoLst1.get(i).overlap(geoLst2.get(i));
                    }
                }
                else result = false;
                break;
            default:
                result = false;
        }
        return result;
    }

    public static Attribute deserialize(ByteStream byteStream) {
        String name = byteStream.readString();
        String type = byteStream.readString();
        return new Attribute(name, type);
    }
}
