package edu.umn.cs.kite.querying.condition;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

/**
 * Created by amr_000 on 1/5/2017.
 */
public class CompoundCondition implements Condition {
    private Condition leftCondition, rightCondition;
    private Condition.LogicalOperator operator;
    private List<Attribute> searchAttributes;
    private final List<Object> plan;

    public CompoundCondition(Condition leftCond,
                             Condition.LogicalOperator operator,
                             Condition rightCond) {
        leftCondition = leftCond;
        rightCondition = rightCond;
        this.operator = operator;
        searchAttributes = new ArrayList<>();
        searchAttributes.addAll(leftCond.getSearchAttributes());
        searchAttributes.addAll(rightCond.getSearchAttributes());
        plan = makeEvaluationPlan();
    }

    private List<Object> makeEvaluationPlan() {
        List<Object> pl = new ArrayList<>();
        pl.addAll(leftCondition.getEvaluationPlan());
        pl.addAll(rightCondition.getEvaluationPlan());
        pl.add(operator);
        return pl;
    }

    @Override public boolean evaluate(HashMap<Attribute,Object> attributes) {
        Stack<Boolean> resultStack = new Stack<>();
        for (int i = 0; i < plan.size(); ++i) {
            BooleanPart part = (BooleanPart) plan.get(i);

            if(part.isSimpleCondition()) {
                SimpleCondition sCondition = (SimpleCondition)part;
                boolean result = sCondition.evaluate(attributes);
                resultStack.push(result);
            } else if(part.isOperator()) {
                LogicalOperator op = (LogicalOperator)part;
                Boolean operand1 = resultStack.pop();
                Boolean operand2 = resultStack.pop();
                Boolean opResult = evaluate(operand1, op, operand2);
                resultStack.push(opResult);
            }
        }
        return resultStack.pop();
    }

    @Override
    public Object getValue(Attribute attribute) {
        Object val = null;
        for (int i = 0; i < plan.size() && val == null; ++i) {
            BooleanPart part = (BooleanPart) plan.get(i);
            if (part.isSimpleCondition()) {
                val = ((SimpleCondition)part).getValue(attribute);
            }
        }
        return val;
    }

    @Override
    public void debug() {
    }

    @Override
    public boolean hasOR() {
        for (int i = 0; i < plan.size(); ++i) {
            BooleanPart part = (BooleanPart) plan.get(i);
            if(part.isOperator()) {
                LogicalOperator op = (LogicalOperator)part;
                if(op == LogicalOperator.OR)
                    return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "CompCond: op="+operator.toString()+" , " +
                "searchArrRef: " + searchAttributes+
                " , size="+searchAttributes.size()+" , " +
                leftCondition.toString() +" , "+ rightCondition.toString();
    }
    private Boolean evaluate(Boolean operand1, LogicalOperator op,
                         Boolean operand2) {
        switch (op) {
            case AND:
                return operand1 && operand2;
            case OR:
                return operand1 || operand2;
            default:
                return false;
        }
    }

    @Override public List<Attribute> getSearchAttributes() {
        return searchAttributes;
    }

    @Override public List<Object> getEvaluationPlan() {return plan;}

    @Override public boolean isSimpleCondition() {return false;}
}
