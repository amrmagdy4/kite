package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.List;

/**
 * Created by amr_000 on 12/27/2016.
 */
public class IndexMetadataEntry implements MetadataEntry {

    private String indexType;
    private String indexName;
    private String streamName;
    private String attributeName;
    private List<Object> options;

    public IndexMetadataEntry(String indexType, String indexName,
                              String streamName, String attributeName,
                              List<Object> options) {
        this.indexType = indexType;
        this.indexName = indexName;
        this.streamName = streamName;
        this.attributeName = attributeName;
        this.options = options;
    }

    @Override public boolean isStreamEntry() {return false;}
    @Override public boolean isIndexEntry() { return true;}
    @Override public boolean isCommandEntry() { return false; }
    @Override public boolean isQueryEntry() { return false; }

    @Override
    public void serialize(ByteStream byteStream) {
        byte metadataType = 1;//0:stream, 1:index
        byteStream.write(metadataType);

        byteStream.write(indexType);
        byteStream.write(indexName);
        byteStream.write(streamName);
        byteStream.write(attributeName);

        int optionsLen = options == null? 0:options.size();
        byteStream.write(optionsLen);
        if(optionsLen > 0) {
            for (int i = 0; i < options.size(); ++i) {
                if (i < 2 || i > 5) {
                    int optionInt = (Integer) options.get(i);
                    byteStream.write(optionInt);
                } else {
                    double optionDbl = (Double) options.get(i);
                    byteStream.write(optionDbl);
                }
            }
        }
    }

    public String getIndexType() {
        return indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public List<Object> getOptions() {
        return options;
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean isMatching(String streamName, String indexName) {
        return streamName.compareTo(getStreamName())==0 && indexName.compareTo
                (getIndexName())==0;
    }

    @Override public String toString() {
        String str = "* Index: "+indexName+"\t(Type: "+indexType+", Stream: " +
                streamName+", Attribute: " + attributeName;
        if(options != null) {
            str += ", Options: ";
            for (int i = 0; i < options.size(); ++i) {
                str += options.get(i).toString();
                if(i < options.size()-1)
                    str += ",";
            }
        }
        str += ")";
        return str;
    }

    public boolean isMatchingStream(String streamName) {
        return streamName.compareTo(getStreamName())==0;
    }
}
