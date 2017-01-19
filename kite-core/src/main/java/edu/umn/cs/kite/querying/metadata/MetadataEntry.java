package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;

/**
 * Created by amr_000 on 12/21/2016.
 */
public interface MetadataEntry {
    boolean isStreamEntry();
    boolean isIndexEntry();
    boolean isCommandEntry();
    boolean isQueryEntry();

    void serialize(ByteStream byteStream);

    static MetadataEntry deserialize(ByteStream byteStream) {
        if(!byteStream.hasRemaining())
            return null;

        MetadataEntry entry = new CommandMetadataEntry("dummy", "dummy");
        byte type = byteStream.readByte();
        switch (type) {
            case 0: //stream
                String streamName = byteStream.readString();
                Scheme scheme = Scheme.deserialize(byteStream);
                StreamSourceInfo source = StreamSourceInfo.deserialize
                        (byteStream);
                StreamFormatInfo format = StreamFormatInfo.deserialize
                        (byteStream);
                entry = new StreamMetadataEntry (streamName,scheme, source,
                        format);
                break;
            case 1: //index
                String indexType = byteStream.readString();
                String indexName = byteStream.readString();
                String indexStreamName = byteStream.readString();
                String attributeName = byteStream.readString();

                ArrayList<Object> options = null;
                int optionsLen = byteStream.readInt();
                if(optionsLen > 0) {
                    options = new ArrayList<>(optionsLen);
                    for (int i = 0; i < optionsLen; ++i) {
                        if (i < 2 || i > 5) {
                            Integer optionInt = byteStream.readInt();
                            options.add(optionInt);
                        } else {
                            Double optionDbl = byteStream.readDouble();
                            options.add(optionDbl);
                        }
                    }
                }
                entry = new IndexMetadataEntry(indexType, indexName,
                        indexStreamName, attributeName, options);
                break;
            default:
                break;
        }
        return entry;
    }
}
