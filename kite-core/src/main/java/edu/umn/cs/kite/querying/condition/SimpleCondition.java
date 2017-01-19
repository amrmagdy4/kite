package edu.umn.cs.kite.querying.condition;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.util.Rectangle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by amr_000 on 1/5/2017.
 */
public class SimpleCondition implements Condition, BooleanPart {
    //Currently, predicate operator is = for non-spatial attribute
    // and WITHIN for spatial attributes
    private Attribute attribute;
    private Object value;
    private List<Attribute> searchAttributes;
    private List<Object> plan;

    public SimpleCondition(Attribute attribute, String attributeVal) {
        init(attribute, attribute.parseValue(attributeVal));
    }

    public SimpleCondition(Attribute attribute, Rectangle range) {
        init(attribute, range);
    }

    private void init(Attribute attribute, Object val) {
        this.attribute = attribute;
        this.value = val;
        searchAttributes = new ArrayList<>();
        searchAttributes.add(attribute);
        plan = makeEvaluationPlan();
    }

    private List<Object> makeEvaluationPlan() {
        List<Object> pl = new ArrayList<>();
        pl.add(this);
        return pl;
    }

    @Override public boolean evaluate(HashMap<Attribute,Object> attributes) {
        Object mVal = attributes.get(attribute);
        if(mVal == null) return false;
        if(attribute.isSpatialAttribute())
            return attribute.overlap(this.value, mVal);
        else
            return attribute.equals (this.value, mVal);
    }

    @Override
    public Object getValue(Attribute attribute) {
        if(attribute.equals(this.attribute))
            return value;
        else
            return null;
    }

    @Override
    public void debug() {
    }

    @Override
    public boolean hasOR() {return false;}

    @Override public String toString() {
        return "SimpleCond: "+attribute.toString()+" , " +
                ""+value.toString()+" searchAttRef="+searchAttributes+" , "+
                "size="+searchAttributes.size();
    }

    @Override
    public List<Attribute> getSearchAttributes() {
        return searchAttributes;
    }

    @Override public List<Object> getEvaluationPlan() {return plan;}

    @Override public boolean isSimpleCondition() {return true;}

    @Override public boolean isOperator() {return false;}
}
