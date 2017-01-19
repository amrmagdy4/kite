package edu.umn.cs.kite.preprocessing;

import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.IdGenerator;
import edu.umn.cs.kite.util.PointLocation;
import edu.umn.cs.kite.util.microblogs.Microblog;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by amr_000 on 8/8/2016.
 */
public class TweetJSONPreprocessor implements Preprocessor<String,Microblog> {
    @Override
    public Microblog preprocess(String jsonTweet, IdGenerator id) {
        return JSON_to_Tweet(jsonTweet, id);
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

    private static Microblog JSON_to_Tweet(String jsonLine, IdGenerator idGen)
    {
        try {
            JSONObject object = new JSONObject(jsonLine);
            //int id = object.getInt("id");
            long id = object.getLong("id");
            Date timestamp = new Date(object.getString("created_at"));
            String text = object.getString("text");

            JSONObject user = object.getJSONObject("user");
            String username = user.getString("screen_name");

            String [] words = text.split("\\s+");
            ArrayList<String> keywords = new ArrayList<String>();
            for(String word:words)
                keywords.add(word);

            double [] coords = getGeoCoordsJSONObj(object);
            GeoLocation loc = coords == null? null:new PointLocation(coords);

            return new Microblog(id, timestamp.getTime(), keywords, loc,
                    username, idGen.nextId());
        } catch(org.json.JSONException e) {
            return null;
        }
    }

    private static double [] getGeoCoordsJSONObj(JSONObject post) {
        //getting geo tag
        double [] coords = new double[2];
        try
        {
            JSONObject geo = post.getJSONObject("geo");
            JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
            coords[0] = coordsJSON.getDouble(0);
            coords[1] = coordsJSON.getDouble(1);
        }
        catch(org.json.JSONException e)
        {
            try
            {
                JSONObject place = post.getJSONObject("place");
                JSONObject placeBoundingBox = place.getJSONObject("bounding_box");

                JSONArray tmpCoords = (JSONArray) placeBoundingBox.get("coordinates");


                double lng =
                        (tmpCoords.getJSONArray(0).getJSONArray(0).getDouble(0) +
                                tmpCoords.getJSONArray(0).getJSONArray(1).getDouble(0) +
                                tmpCoords.getJSONArray(0).getJSONArray(2).getDouble(0) +
                                tmpCoords.getJSONArray(0).getJSONArray(3).getDouble(0))/4;
                double lat =
                        (tmpCoords.getJSONArray(0).getJSONArray(0).getDouble(1) +
                                tmpCoords.getJSONArray(0).getJSONArray(1).getDouble(1) +
                                tmpCoords.getJSONArray(0).getJSONArray(2).getDouble(1) +
                                tmpCoords.getJSONArray(0).getJSONArray(3).getDouble(1))/4;
                coords[0] = lat;
                coords[1] = lng;
            }
            catch(org.json.JSONException e2)
            {
                coords = null;
                return coords;
            }
        }
        return coords;
    }
}
