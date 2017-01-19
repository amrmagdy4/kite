package edu.umn.cs.kite.streaming;

import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.preprocessing.Preprocessor;
import edu.umn.cs.kite.querying.metadata.StreamFormatInfo;
import edu.umn.cs.kite.querying.metadata.StreamSourceInfo;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.io.IOException;
import java.util.List;

/**
 * Created by amr_000 on 8/31/2016.
 */
public abstract class StreamingDataSource {
    private Scheme scheme = null;

    public abstract List<String> getData() throws IOException;
    public abstract Preprocessor<String,Microblog> getPreprocessor();

    public static StreamingDataSource create(StreamSourceInfo streamSourceInfo,
                                      Scheme scheme,
                                      StreamFormatInfo streamFormatInfo) {
        Preprocessor<String,Microblog> preprocessor = streamFormatInfo
                .createPreprocessor(scheme);
        return streamSourceInfo.getStreamSource(scheme,preprocessor);
    }

    public void setScheme(Scheme scheme) {this.scheme = scheme;}
    public Scheme getScheme() {return scheme;}

    public abstract String toString();
}
