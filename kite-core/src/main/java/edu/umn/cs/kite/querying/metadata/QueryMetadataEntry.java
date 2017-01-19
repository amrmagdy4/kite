package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.querying.Query;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;

/**
 * Created by amr_000 on 12/30/2016.
 */
public class QueryMetadataEntry implements MetadataEntry {

    private Query query;
    private ArrayList<String> attributeNames;
    private String streamName;

    public QueryMetadataEntry(String streamName, ArrayList<String>
            attributeNames, Query query) {
        this.streamName = streamName;
        this.attributeNames = attributeNames;
        this.query = query;
    }

    @Override
    public boolean isStreamEntry() {return false;}
    @Override
    public boolean isIndexEntry() {return false;}
    @Override
    public boolean isCommandEntry() {return false;}
    @Override
    public boolean isQueryEntry() {return true;}

    @Override
    public void serialize(ByteStream byteStream) {
        //no need to serialize a query
    }

    public String getStreamName() {
        return streamName;
    }

    public Query getQuery() {
        return query;
    }

    public ArrayList<String> getAttributeNames() {
        return attributeNames;
    }
}
