package edu.umn.cs.kite.util.microblogs;

/**
 * Created by amr_000 on 8/1/2016.
 */

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.querying.condition.Condition;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.io.Serializable;
import java.util.*;

public class Microblog implements Serializable {
    //private long id;
    private Long timestamp;
    //private ArrayList<String> keywords;
    //private GeoLocation geolocation;
    //private String username;

    private HashMap<Attribute,Object> attributes;

    //system fields
    private long kiteId;
    //private static long currKiteId = 0;

    public Microblog(HashMap<Attribute,Object> attributes, long systemKey) {
        this.attributes = attributes;
        kiteId = systemKey;

        //assign timestamp
        timestamp = (Long)getAttributeValue("timestamp");
        if(timestamp == null)
            timestamp = new Date().getTime();
    }

    public Microblog(long id, long timestamp, ArrayList<String> keywords,
                     GeoLocation loc, String user, long systemKey) {
        //this.id = id;
        //this.timestamp = timestamp;
        //this.keywords = keywords;
        //this.geolocation = loc;
        //this.username = user;

        attributes = new HashMap<Attribute,Object>();

        kiteId = systemKey;
    }

    public long getId() { return kiteId; }


    public String toString()
    {
        String string = "";
        Set<Attribute> mAttributes = this.getAttributes();
        for(Attribute attribute: mAttributes) {
            String attributeName = attribute.getName();
            Object val = this.getAttributeValue(attributeName);
            List<String> strLst = attribute.getStringListValue(val);
            String valStr = "";
            for(String str: strLst)
                valStr += str+",";
            string += valStr;
        }
        return string;
    }

    public int serialize(ByteStream byteStream, Scheme scheme) {
        int capacity = 1400;
        ByteStream tempByteStream = new ByteStream(capacity);

        //serialize record attributes
        ArrayList<Attribute> schemeAttributes = scheme.getAttributes();
        for(int i = 0; i < schemeAttributes.size(); ++i) {
            Object val = this.attributes.get(schemeAttributes.get(i));
            //if(val == null)
            //    throw new IllegalArgumentException("Error in Microblog " +
            //            "record serialization. Mismatch scheme attributes.")
            schemeAttributes.get(i).serializeVal(val, tempByteStream);
        }

        //serialize system fields
        tempByteStream.write(kiteId);

        byte [] microblogBytes = tempByteStream.getBytes();
        int microblogBytesLen = tempByteStream.getWrittenBytes();

        int bytesLen = 0;
        bytesLen += byteStream.write(microblogBytesLen);
        bytesLen += byteStream.write(microblogBytes, microblogBytesLen);

        return bytesLen;
    }

    public static boolean skipMicroblog(ByteStream byteStream) {
        if(!byteStream.hasRemaining())
            return false;

        int microblogBytesLen = byteStream.readInt();
        return byteStream.skipBytes(microblogBytesLen);
    }

    public static Microblog deserialize(ByteStream byteStream, Scheme scheme) {
        if(!byteStream.hasRemaining())
            return null;

        int microblogBytesLen = byteStream.readInt();
        byte [] bytes = byteStream.readBytes(microblogBytesLen);


        ByteStream tmpByteStream = new ByteStream(bytes);
        HashMap<Attribute,Object> attributes = new HashMap<>();
        ArrayList<Attribute> schemeAttributes = scheme.getAttributes();
        for(int i = 0; i < schemeAttributes.size(); ++i) {
            Object val = schemeAttributes.get(i).deserializeVal(tmpByteStream);
            attributes.put(schemeAttributes.get(i), val);
        }

        long kiteId = tmpByteStream.readLong();

        Microblog microblog = new Microblog(attributes, kiteId);
        return microblog;
    }

    public long getTimestamp() { return timestamp; }

    public Object getAttributeValue(Attribute attribute) {
        return attributes.get(attribute);
    }

    public Object getAttributeValue(String attributeName) {
        for(Attribute attribute: attributes.keySet())
            if(attribute.getName().compareTo(attributeName) == 0)
                return attributes.get(attribute);
        return null;
    }

    public Attribute getAttribute(String attributeName) {
        for(Attribute attribute: attributes.keySet())
            if(attribute.getName().compareTo(attributeName) == 0)
                return attribute;
        return null;
    }

    public List<String> getKeysStr(Attribute indexedAttribute) {
        List<String> keys = indexedAttribute.getStringListValue(attributes
                .get(indexedAttribute));
        return keys;
    }

    public List<GeoLocation> getKeysGeo(Attribute indexedAttribute) {
        List<GeoLocation> keys = indexedAttribute.getGeoListValue(attributes
                .get(indexedAttribute));
        return keys;
    }

    public boolean overlap(Rectangle range, Attribute spatialAttribute) {
        GeoLocation loc = (GeoLocation)attributes.get(spatialAttribute);
        return loc.overlap(range);
    }

    public GeoLocation getLocation(Attribute spatialAttribute) {
        return (GeoLocation)attributes.get(spatialAttribute);
    }

    public Set<Attribute> getAttributes() {
        return attributes.keySet();
    }

    public boolean match(Condition condition) {
        return condition.evaluate(this.attributes);
    }
}
