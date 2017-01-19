package edu.umn.cs.kite.preprocessing;

import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.querying.metadata.StreamFormatInfo;
import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.IdGenerator;
import edu.umn.cs.kite.util.PointLocation;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by amr_000 on 8/8/2016.
 */
public class MicroblogCSVPreprocessor implements Preprocessor<String,Microblog> {

    private ArrayList<Integer> indices;
    private Scheme scheme;

    public MicroblogCSVPreprocessor(StreamFormatInfo format, Scheme scheme) {
        if(format.isCSV()) {
            this.indices = (ArrayList<Integer>) format.getAttributeMap();
            this.scheme = scheme;

            if(indices.size() != scheme.numAttributes())
                throw new IllegalArgumentException("Invalid format or scheme." +
                        " Incompatible number of attributes in the scheme " +
                        "and CSV format list.");
        } else
            throw new IllegalArgumentException("Invalid format type. CSV " +
                    "format is required to instantiate CSV Preprocessor.");
    }

    @Override
    public Microblog preprocess(String csvTweet, IdGenerator id) {
        return CSV_to_Microblog(csvTweet, id);
    }

    @Override
    public List<Microblog> preprocess(List<String> objects, IdGenerator id) {
        List<Microblog> microblogs = new ArrayList<>(objects.size());
        for(String line : objects) {
            Microblog microblog = preprocess(line, id);
            if(microblog != null)
                microblogs.add(microblog);
        }
        return microblogs;
    }

    private Microblog CSV_to_Microblog(String csvLine, IdGenerator idGen)
    {
        String [] fields = csvLine.split(",");

        HashMap<Attribute,Object> attributesValues = new HashMap<>();
        ArrayList<Attribute> attributes = scheme.getAttributes();
        for(int i = 0; i < attributes.size(); ++i) {
            Object value = indices.get(i) < fields.length? attributes.get(i)
                    .parseValue(fields[indices.get(i)]):null;

            if(attributes.get(i).isList()) {
                ArrayList<Object> listValue = new ArrayList<>();
                if(value != null)
                    listValue.add(value);

                for(int j = indices.get(i)+1; j < fields.length; ++j) {
                    value = attributes.get(i).parseValue(fields[j]);
                    listValue.add(value);
                }
                attributesValues.put(attributes.get(i), listValue);
            }
            else attributesValues.put(attributes.get(i), value);
        }

        return new Microblog(attributesValues, idGen.nextId());
    }

    private Microblog CSV_to_Tweet(String csvLine, IdGenerator idGen)
    {
        String [] fields = csvLine.split(",");
        long id = Long.parseLong(fields[0]);
        Date timestamp = new Date(Long.parseLong(fields[1]));
        String username = fields[2];

        GeoLocation loc;
        String locType = fields[3];
        int textIndex = 4;
        if(locType.compareTo("null") == 0)
            loc = null;
        else if(locType.compareTo("2") == 0) {// 2 coordinates(point)
            textIndex += 2;
            loc = new PointLocation(Double.parseDouble(fields[4]),
                    Double.parseDouble(fields[5]));
        }
        else if(locType.compareTo("4") == 0) {// 4 coordinates(rectangle)
            textIndex += 4;
            loc = new Rectangle(Double.parseDouble(fields[4]),
                    Double.parseDouble(fields[5]),Double.parseDouble(fields[6]),
                    Double.parseDouble(fields[7]));
        }
        else
            loc = null;

        ArrayList<String> keywords = new ArrayList<String>();


        for(int i = textIndex; i < fields.length; ++i)
            keywords.add(fields[i]);

        return new Microblog(id, timestamp.getTime(), keywords, loc, username,
                idGen.nextId());
    }
}
