package edu.umn.cs.kite.querying;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.io.PrintStream;
import java.util.List;

/**
 * Created by amr_000 on 1/4/2017.
 */
public class MQLResults implements Runnable {
    private List<Microblog> answer;
    private List<String> projectedAttributesNames;
    private boolean oneLineReady = false;
    private String oneLine;

    public MQLResults(List<Microblog> answer, List<String> attributeNames) {
        this.answer = answer;
        projectedAttributesNames = attributeNames;
    }

    @Override
    public void run() {
        formOneLineResults();
    }

    private void formOneLineResults() {
        oneLine = "";
        if (projectedAttributesNames.get(0).compareTo("*") == 0) {
            for (Microblog microblog : answer) {
                oneLine += microblog.toString();
                oneLine += System.lineSeparator();
            }
        } else {
            for (Microblog microblog : answer) {
                for (String attributeName : projectedAttributesNames) {
                    Attribute attribute = microblog.getAttribute(attributeName);
                    Object val = microblog.getAttributeValue(attributeName);
                    List<String> strLst = attribute.getStringListValue(val);
                    String valStr = "";
                    for (String str : strLst)
                        valStr += str + ",";
                    oneLine += valStr;
                }
                oneLine += System.lineSeparator();
            }
        }
        oneLineReady = true;
    }

    public void print(PrintStream printStream) {
        printStream.println(this.toString());
    }

    public void print() {
        print(System.out);
    }

    public String toString() {
        if(!oneLineReady)
            formOneLineResults();
        return oneLine;
    }

    public int getAnswerSize() {
        return answer.size();
    }
}
