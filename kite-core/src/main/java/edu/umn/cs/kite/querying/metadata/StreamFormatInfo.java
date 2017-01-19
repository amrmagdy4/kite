package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.preprocessing.MicroblogCSVPreprocessor;
import edu.umn.cs.kite.preprocessing.Preprocessor;
import edu.umn.cs.kite.util.microblogs.Microblog;
import edu.umn.cs.kite.util.serialization.ByteStream;
import edu.umn.cs.kite.util.serialization.Serializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

/**
 * Created by amr_000 on 12/21/2016.
 */
public class StreamFormatInfo {

    private String formatType;
    private Hashtable<String,Object> info;

    private static HashSet<String> formatTypes;
    static {
        formatTypes = new HashSet<>();
        formatTypes.add("csv");
    }

    public StreamFormatInfo(String type, ArrayList<Integer> attrIndecies) {
        formatType = type;
        info = new Hashtable<>();
        info.put("indicies",attrIndecies);
    }

    public static boolean isValidStreamFormat(String formatType) {
        return formatTypes.contains(formatType.toLowerCase());
    }

    public static StreamFormatInfo getStreamFormatInfo(String formatType,
                                                       String formatList) {
        switch (formatType) {
            case "csv":
                return csvSource(formatList);
            default:
                return null;
        }
    }

    private static StreamFormatInfo csvSource(String formatList) {
        String [] attributesIndeciesStrs = formatList.split("\\s*,\\s*");
        ArrayList<Integer> attributesIndecies = new ArrayList<>();
        for(int j = 0; j < attributesIndeciesStrs.length; ++j) {
            try {
                int ind = Integer.parseInt(attributesIndeciesStrs[j]);
                attributesIndecies.add(ind);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return new StreamFormatInfo("csv", attributesIndecies);
    }

    public boolean isCompatible(ArrayList<Attribute> attributes) {
        switch (formatType) {
            case "csv":
                ArrayList<Integer> attrIndecies = (ArrayList<Integer>) info
                        .get("indicies");
                return attributes.size() == attrIndecies.size();
            default:
                return false;
        }
    }

    public Object getAttributeMap() {
        switch (formatType) {
            case "csv":
                ArrayList<Integer> attrIndecies = (ArrayList<Integer>) info
                        .get("indicies");
                return attrIndecies;
            default:
                return null;
        }
    }

    public boolean isCSV() {
        return formatType.compareTo("csv")==0;
    }

    public Preprocessor<String,Microblog> createPreprocessor(Scheme scheme) {
        switch (formatType) {
            case "csv":
               return new MicroblogCSVPreprocessor(this, scheme);
            default:
                return null;
        }
    }

    public void serialize(ByteStream byteStream) {
        byteStream.write(formatType);
        switch (formatType) {
            case "csv":
                ArrayList<Integer> attrIndecies = (ArrayList<Integer>) info
                        .get("indicies");
                Serializer.serializeIntList(attrIndecies, byteStream);
            default:;
        }
    }

    public static StreamFormatInfo deserialize(ByteStream byteStream) {
        String formatType = byteStream.readString();
        switch (formatType) {
            case "csv":
                ArrayList<Integer> attrIndecies = Serializer
                        .deserializeIntList(byteStream);
                return new StreamFormatInfo(formatType, attrIndecies);
            default:
                return null;
        }
    }

    @Override public String toString(){
        String str = formatType;
        for(String key: info.keySet())
            str += ","+key+":"+info.get(key).toString();
        return str;
    }
}
