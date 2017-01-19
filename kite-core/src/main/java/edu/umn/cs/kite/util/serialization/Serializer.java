package edu.umn.cs.kite.util.serialization;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartition;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.microblogs.Microblog;
import javafx.util.Pair;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Created by amr_000 on 8/25/2016.
 */
public class Serializer {

    public static Pair<Integer, Integer> serializeHashEntry(
            Cache.Entry<String, ArrayList<Long>> entry, StreamDataset stream,
            ByteStream recordBytes, int entryInd) {
        int bytesLen = 0;
        bytesLen += serializeKey(entry.getKey(), recordBytes);
        Pair<Integer,Integer> results = serializeMemoryValue(entry.getValue(),
                stream, recordBytes, entryInd);
        return new Pair<Integer,Integer>(results.getKey()+bytesLen,
                results.getValue());
    }

    private static int serializeKey(String key, ByteStream recordBytes) {
        int bytesLen = 0;
        byte type = 0;
        //key type, 0 is String, 1 is Spatial
        bytesLen += recordBytes.write(type);
        bytesLen += recordBytes.write(key);
        return bytesLen;
    }

    private static int serializeKeyGeo(Integer spatialPartitionId, ByteStream
            recordBytes) {
        int bytesLen = 0;
        byte type = 1;
        //key type, 0 is String, 1 is Spatial
        bytesLen += recordBytes.write(type);
        bytesLen += recordBytes.write(spatialPartitionId);
        return bytesLen;
    }

    private static Pair<Integer,Integer> serializeMemoryValue(ArrayList<Long>
                         idList, StreamDataset stream, ByteStream recordBytes,
                                                              int startInd) {
        int bytesLen = 0;
        for(int i = startInd; i < idList.size(); ++i) {
            Long mid = idList.get(i);
            Microblog microblog = stream.getRecord(mid);

            bytesLen += microblog.serialize(recordBytes, stream.getScheme());
            //null pointer
            // exception here, mostly during index flushing, looks like microblog is null for some reason (may be related to discard method)
            stream.decrementIndexesCount(microblog.getId());

            if(bytesLen >= ConstantsAndDefaults.FILE_BLK_SIZE_BYTES) {
                if((i+1) < idList.size())
                    return new Pair<Integer, Integer>(bytesLen, i + 1);
            }
        }
        return new Pair<Integer,Integer>(bytesLen,-1);
    }

    public static int serializeGeoLocation(GeoLocation geolocation,
                                           ByteStream byteStream) {
        if(geolocation == null)
            return GeoLocation.serializeNull(byteStream);
        else
            return geolocation.serialize(byteStream);
    }

    public static int serializeStringList(List<String> strings, ByteStream
            byteStream) {
        int bytesLen = 0;
        int listLen = strings == null? 0: strings.size();
        bytesLen += byteStream.write(listLen);
        for(int i = 0; i < listLen; ++i)
            bytesLen += byteStream.write(strings.get(i));
        return bytesLen;
    }

    public static int serializeLongList(List<Long> longs, ByteStream
            byteStream) {
        int bytesLen = 0;
        int listLen = longs == null? 0:longs.size();
        bytesLen += byteStream.write(listLen);
        for(int i = 0; i < listLen; ++i)
            bytesLen += byteStream.write(longs.get(i));
        return bytesLen;
    }

    public static int serializeGeoLocationList(List<GeoLocation> locs,
                                               ByteStream byteStream) {
        int bytesLen = 0;
        int listLen = locs==null? 0:locs.size();
        bytesLen += byteStream.write(listLen);
        for(int i = 0; i < listLen; ++i)
            bytesLen += locs.get(i).serialize(byteStream);
        return bytesLen;
    }

    public static ArrayList<Long> deserializeLongList(ByteStream byteStream) {
        int listSize = byteStream.readInt();
        ArrayList<Long> list = new ArrayList<>(listSize);

        for(int i = 0; i < listSize; ++i) {
            list.add(byteStream.readLong());
        }

        return list;
    }

    public static ArrayList<Integer> deserializeIntList(ByteStream byteStream) {
        int listSize = byteStream.readInt();
        ArrayList<Integer> list = new ArrayList<>(listSize);
        for(int i = 0; i < listSize; ++i)
            list.add(byteStream.readInt());
        return list;
    }

    public static ArrayList<String> deserializeStringList(ByteStream byteStream) {
        int listSize = byteStream.readInt();
        ArrayList<String> list = new ArrayList<>(listSize);

        for(int i = 0; i < listSize; ++i) {
            list.add(byteStream.readString());
        }

        return list;
    }

    public static ArrayList<GeoLocation> deserializeGeoLocationList(ByteStream
                                                                 byteStream) {
        int listSize = byteStream.readInt();
        ArrayList<GeoLocation> list = new ArrayList<>(listSize);

        for(int i = 0; i < listSize; ++i) {
            list.add(GeoLocation.deserialize(byteStream));
        }

        return list;
    }

    public static Pair<Integer, Integer> serializeSpatialEntry(
            Cache.Entry<Integer, SpatialPartition> entry, StreamDataset stream,
            ByteStream recordBytes, int entryInd) {
        int bytesLen = 0;
        bytesLen += serializeKeyGeo(entry.getKey(), recordBytes);
        Pair<Integer,Integer> results = serializeMemoryValue(
                entry.getValue().getData(), stream, recordBytes, entryInd);
        return new Pair<Integer,Integer>(results.getKey()+bytesLen,
                results.getValue());
    }

    public static Pair<Integer, ArrayList<Microblog>>
                            deserializeSpatialEntry(byte[] keyData,
                                                    Scheme scheme) {
        ByteStream byteStream = new ByteStream(keyData);
        Pair<Integer,Integer> result = deserializeKeyGeo(byteStream);
        Integer deserializedPartitionId = result.getKey();
        int deserializedLen = result.getValue();

        ArrayList<Microblog> microblogs = deserializeValue(byteStream, scheme);

        Pair<Integer, ArrayList<Microblog>> singleHashEntry = new Pair<>
        (deserializedPartitionId, microblogs);

        return singleHashEntry;
    }

    public static Hashtable<String,ArrayList<Microblog>> deserializeHashEntry(
            byte[] keyData, Scheme scheme) {

        ByteStream byteStream = new ByteStream(keyData);
        int offset = 0;
        Pair<String,Integer> result = deserializeKey(byteStream);
        String deserializedKey = result.getKey();
        int deserializedLen = result.getValue();
        offset = deserializedLen;

        ArrayList<Microblog> microblogs = deserializeValue(byteStream, scheme);

        Hashtable<String, ArrayList<Microblog>> singleHashEntry = new
                Hashtable<>();
        singleHashEntry.put(deserializedKey, microblogs);
        return singleHashEntry;
    }

    private static ArrayList<Microblog> deserializeValue(ByteStream
                                        byteStream, Scheme scheme)
    {
        ArrayList<Microblog> microblogs = new ArrayList<>();
        Microblog tmpMicroblog;
        while ((tmpMicroblog = Microblog.deserialize(byteStream,scheme))!=null)
            microblogs.add(tmpMicroblog);
        return microblogs;
    }

    private static Pair<String,Integer> deserializeKey(ByteStream byteStream) {
        int positionBefore = byteStream.getReadBytes();
        byte keyType = byteStream.readByte();
        String key = byteStream.readString();
        int positionAfter = byteStream.getReadBytes();
        int deserializedLen = positionAfter-positionBefore;

        return new Pair<>(key,deserializedLen);
    }

    private static Pair<Integer,Integer> deserializeKeyGeo(ByteStream byteStream)
    {
        int positionBefore = byteStream.getReadBytes();
        byte keyType = byteStream.readByte();
        Integer key = byteStream.readInt();
        int positionAfter = byteStream.getReadBytes();
        int deserializedLen = positionAfter-positionBefore;

        return new Pair<>(key,deserializedLen);
    }


    public static void serializeAttributeList(ArrayList<Attribute> attributes,
                                              ByteStream byteStream) {
        int listLen = attributes.size();
        byteStream.write(listLen);
        for(int i = 0; i < listLen; ++i)
            attributes.get(i).serialize(byteStream);
    }

    public static int serializeIntList(ArrayList<Integer> ints,
                                        ByteStream byteStream) {
        int bytesLen = 0;
        int listLen = ints == null? 0:ints.size();
        bytesLen += byteStream.write(listLen);
        for(int i = 0; i < listLen; ++i)
            bytesLen += byteStream.write(ints.get(i));
        return bytesLen;
    }

    public static ArrayList<Attribute> deserializeAttributeList(ByteStream
                                                                byteStream) {
        int listLen = byteStream.readInt();
        ArrayList<Attribute> list = new ArrayList<>(listLen);
        for(int i = 0; i < listLen; ++i)
            list.add(Attribute.deserialize(byteStream));
        return list;
    }
}
