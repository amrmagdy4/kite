package edu.umn.cs.kite.querying.condition;

/**
 * Created by amr_000 on 1/9/2017.
 */
public class BooleanResult implements BooleanPart {

    private boolean result;

    public BooleanResult(boolean result) { this.result = result; }

    @Override public boolean isSimpleCondition() {return false;}

    @Override public boolean isOperator() {return false;}
}
