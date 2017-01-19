package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.preprocessing.Preprocessor;
import edu.umn.cs.kite.streaming.SocketStream;
import edu.umn.cs.kite.streaming.StreamingDataSource;
import edu.umn.cs.kite.util.microblogs.Microblog;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.regex.Pattern;

/**
 * Created by amr_000 on 12/21/2016.
 */
public class StreamSourceInfo {
    private String sourceType;
    private Hashtable<String,Object> info;

    private static final Pattern ipV4Pattern = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    private static HashSet<String> sourceTypes;
    static {
        sourceTypes = new HashSet<>();
        sourceTypes.add("network_tcp");
    }

    public StreamSourceInfo(String type, String ip, int port) {
        sourceType = type;
        info = new Hashtable<>();
        info.put("host",ip);
        info.put("port",port);
    }

    public static boolean isValidStreamSource(String sourceType) {
        return sourceTypes.contains(sourceType.toLowerCase());
    }

    public StreamingDataSource getStreamSource( Scheme scheme,
            Preprocessor<String,Microblog> preprocessor) {
        switch (sourceType) {
            case "network_tcp":
                String host = (String)info.get("host");
                Integer port = (Integer) info.get("port");
                return new SocketStream(scheme, host, port, preprocessor);
            default:
                return null;
        }
    }

    public static StreamSourceInfo getStreamSourceInfo(String sourceType, String
            sourceCredentials) {
        switch (sourceType) {
            case "network_tcp":
                return networkTCPSource(sourceCredentials);
            default:
                return null;
        }
    }

    private static StreamSourceInfo networkTCPSource(String sourceCredentials) {
        String [] credentials = sourceCredentials.split(":");
        if(credentials.length != 2) return null;
        String ip = credentials[0];
        int port;
        String portStr = credentials[1];
        if(!ipV4Pattern.matcher(ip).matches()) return null;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            return null;
        }
        return new StreamSourceInfo("network_tcp", ip, port);
    }

    public void serialize(ByteStream byteStream) {
        byteStream.write(sourceType);
        switch (sourceType) {
            case "network_tcp":
                String host = (String) info.get("host");
                Integer port = (Integer) info.get("port");
                byteStream.write(host);
                byteStream.write(port);
            default:;
        }
    }

    public static StreamSourceInfo deserialize(ByteStream byteStream) {
        String sourceType = byteStream.readString();
        switch (sourceType) {
            case "network_tcp":
                String host = byteStream.readString();
                Integer port = byteStream.readInt();
                return new StreamSourceInfo(sourceType, host, port);
            default:
                return null;
        }
    }

    @Override public String toString(){
        String str = sourceType;
        for(String key: info.keySet())
            str += ","+key+":"+info.get(key).toString();
        return str;
    }
}
