package edu.umn.cs.kite.datamodel;

import edu.umn.cs.kite.util.serialization.ByteStream;
import edu.umn.cs.kite.util.serialization.Serializer;

import java.util.ArrayList;

/**
 * Created by amr_000 on 12/20/2016.
 */
public class Scheme {
    private ArrayList<Attribute> attributes;

    public Scheme (ArrayList<Attribute> attributesList) {
        attributes = attributesList;
    }
    public int numAttributes() {return attributes.size();}

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public boolean attributeExists(String attributeName) {
        for(Attribute attribute: attributes) {
            if(attribute.getName().compareTo(attributeName) == 0)
                return true;
        }
        return false;
    }

    public boolean spatialAttributeExists(String attributeName) {
        for(Attribute attribute: attributes) {
            if(attribute.getName().compareTo(attributeName) == 0
                    && attribute.isSpatialAttribute())
                return true;
        }
        return false;
    }

    public Attribute getAttribute(String attributeName) {
        for(Attribute attribute: attributes) {
            if(attribute.getName().compareTo(attributeName) == 0)
                return attribute;
        }
        return null;
    }

    public void serialize(ByteStream byteStream) {
        Serializer.serializeAttributeList(this.getAttributes(), byteStream);
    }

    public static Scheme deserialize(ByteStream byteStream) {
        ArrayList<Attribute> attributes = Serializer.deserializeAttributeList
                (byteStream);
        return new Scheme(attributes);
    }

    public String toString() {
        String str = "";
        for(Attribute attribute: attributes) {
            str += attribute.toString();
        }
        return str;
    }
}
