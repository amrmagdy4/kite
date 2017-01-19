package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;

/**
 * Created by amr_000 on 12/21/2016.
 */
public class StreamMetadataEntry implements MetadataEntry {

    private String streamName;
    private Scheme scheme;
    private StreamSourceInfo source;
    private StreamFormatInfo format;

    public StreamMetadataEntry(String streamName, ArrayList<Attribute>
            attributes, StreamSourceInfo source, StreamFormatInfo format) {
        this(streamName, new Scheme(attributes), source, format);
    }

    public StreamMetadataEntry(String streamName, Scheme scheme,
                             StreamSourceInfo source, StreamFormatInfo format) {
        this.streamName = streamName;
        this.scheme = scheme;
        this.source = source;
        this.format = format;
    }

    public String getStreamName() {return streamName;}
    public Scheme getScheme() {return scheme;}
    public StreamSourceInfo getStreamSourceInfo() {return source;}
    public StreamFormatInfo getStreamFormatInfo() {return format;}

    @Override public boolean isStreamEntry() {return true;}
    @Override public boolean isIndexEntry(){return false;}
    @Override public boolean isCommandEntry() { return false; }
    @Override public boolean isQueryEntry() {return false;}

    @Override
    public void serialize(ByteStream byteStream) {
        byte metadataType = 0;//0:stream, 1:index
        byteStream.write(metadataType);
        byteStream.write(streamName);
        scheme.serialize(byteStream);
        source.serialize(byteStream);
        format.serialize(byteStream);
    }

    public boolean isMatchingName(String name) {
        return streamName.compareTo(name) == 0;
    }

    public boolean attributeExists(String attributeName) {
        return scheme.attributeExists(attributeName);
    }

    public boolean isSpatialAttributeExists(String attributeName) {
        return scheme.spatialAttributeExists(attributeName);
    }

    public Attribute getAttribute(String attributeName) {
        return scheme.getAttribute(attributeName);
    }

    @Override public String toString() {
        String str = "* Stream: "+streamName;
        str += System.lineSeparator();
        str += "\t\t";
        str += "Scheme: "+scheme.toString();
        str += System.lineSeparator();
        str += "\t\t";
        str += "Source: "+source.toString();
        str += System.lineSeparator();
        str += "\t\t";
        str += "Format: "+format.toString();
        return str;
    }
}
