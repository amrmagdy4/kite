package edu.umn.cs.kite.querying;

import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.querying.condition.Condition;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.TemporalPeriod;

import java.util.Hashtable;

/**
 * Created by amr_000 on 12/5/2016.
 */
public class Query {
    private QueryType queryType;
    private Condition condition = null;
    private Attribute primaryAttribute = null;
    private Object primaryAttributeValue = null;

    private Hashtable<String, Object> params;
    private TemporalPeriod time = ConstantsAndDefaults.QUERY_TIME();
    private int k = ConstantsAndDefaults.QUERY_ANSWER_SIZE;

    public Query(QueryType queryType) {
        this.queryType = queryType;
        params = new Hashtable<>();
    }

    @Override public String toString() {
        return "<"+queryType+","+condition+","+primaryAttribute+","+
                primaryAttributeValue+">";
    }
    public Query(QueryType type, Condition condition) {
        this.queryType = type;
        this.condition = condition;
    }

    public QueryType getQueryType() { return queryType; }

    public Rectangle getParam_Rectangle(String paramName) {
        Rectangle spatialRect = (Rectangle)params.get(paramName);
        return spatialRect;
    }

    public void putParam_Rectangle(String paramName, Rectangle rect) {
        params.put(paramName, rect);
    }

    public boolean isHash() {
        return queryType==QueryType.HASH_TEMPORAL;
    }

    public boolean isSpatial() {
        return queryType== QueryType.SPATIAL_RANGE_TEMPORAL || queryType== QueryType
        .SPATIAL_KNN_TEMPORAL;
    }

    public boolean isSpatialRange() {
        return queryType== QueryType.SPATIAL_RANGE_TEMPORAL;
    }

    public boolean isSpatialKNN() {
        return queryType== QueryType.SPATIAL_KNN_TEMPORAL;
    }

    public int getK() {
        return k;
    }

    public TemporalPeriod getTime() {
        return time;
    }

    public void setK(int k) {
        this.k = k;
    }

    public void setTime(TemporalPeriod time) {
        this.time = time;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public void setPrimaryAttribute(Attribute primaryAttribute) {
        this.primaryAttribute = primaryAttribute;
    }

    public Attribute getPrimaryAttribute() {
        return primaryAttribute;
    }

    public boolean isSingleSearchAttribute() {
        return getCondition().getSearchAttributes().size()==1;
    }

    public void setPrimaryAttributeValue(Object primaryAttributeValue) {
        this.primaryAttributeValue = primaryAttributeValue;
    }

    public Object getPrimaryAttributeValue() {
        return primaryAttributeValue;
    }
}
