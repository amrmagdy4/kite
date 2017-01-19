package edu.umn.cs.kite.querying.condition;

/**
 * Created by amr_000 on 1/9/2017.
 */
public interface BooleanPart {
    boolean isSimpleCondition();
    boolean isOperator();
}
