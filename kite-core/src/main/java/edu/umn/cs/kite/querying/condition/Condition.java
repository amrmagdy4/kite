package edu.umn.cs.kite.querying.condition;

import edu.umn.cs.kite.datamodel.Attribute;

import java.util.HashMap;
import java.util.List;

/**
 * Created by amr_000 on 1/5/2017.
 */
public interface Condition {
    List<Attribute> getSearchAttributes();
    List<Object> getEvaluationPlan();
    boolean evaluate(HashMap<Attribute,Object> attributes);

    Object getValue(Attribute attribute);

    void debug();

    boolean hasOR();

    enum LogicalOperator implements BooleanPart {
        OR {@Override public boolean isSimpleCondition() {return false;}
            @Override public boolean isOperator() {return true;}},
        AND {@Override public boolean isSimpleCondition() {return false;}
            @Override public boolean isOperator() {return true;}},
        INVALID {@Override public boolean isSimpleCondition() {return false;}
            @Override public boolean isOperator() {return true;}}
    };
    boolean isSimpleCondition();
}
