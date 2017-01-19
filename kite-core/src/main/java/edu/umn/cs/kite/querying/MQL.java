package edu.umn.cs.kite.querying;

import edu.umn.cs.kite.common.DebugFlagger;
import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.datamodel.Attribute;
import edu.umn.cs.kite.indexing.memory.spatial.GridPartitioner;
import edu.umn.cs.kite.indexing.memory.spatial.SpatialPartitioner;
import edu.umn.cs.kite.querying.condition.CompoundCondition;
import edu.umn.cs.kite.querying.condition.Condition;
import edu.umn.cs.kite.querying.condition.SimpleCondition;
import edu.umn.cs.kite.querying.metadata.*;
import edu.umn.cs.kite.streaming.StreamDataset;
import edu.umn.cs.kite.streaming.StreamingDataSource;
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.TemporalPeriod;
import javafx.util.Pair;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by amr_000 on 12/20/2016.
 */
//primary version of informal MQL parser, to be enhanced
public class MQL {
    private static HashSet<String> MQLAllKeywords;
    private static HashSet<String> MQLStartingKeywords ;
    private static HashSet<String> MQLCreateStatKeywords;
    private static HashSet<String> MQLSelectStatKeywords;

    //Regex patterns
    private static final Pattern idPattern = Pattern.compile("\\w+");
    private static final Pattern idWtUnderscorePattern =
            Pattern.compile("[a-zA-Z0-9]+");
    //create\s+stream\s+[a-zA-Z0-9]+\s+\(\w\s*[,\w]*\)
    //create     stream    st23    (attr1,attr2,attr3,attr4)

    //Keywords
    private static final String dropKeyword = "drop";
    private static final String createKeyword = "create";
    private static final String streamKeyword = "stream";
    private static final String indexKeyword = "index";
    private static Object[] indexOptionsDefaultValues = {ConstantsAndDefaults
           .TEMPORAL_FLUSHING_PERIODIC_DATA_SIZE, ConstantsAndDefaults.
           MEMORY_INDEX_DEFAULT_SEGMENTS, 90.0, -90.0, 180.0, -180.0, 180, 360};
    private static final String indexHashKeyword = "hash";
    private static final String indexSpatialKeyword = "spatial";
    private static final String onKeyword = "on";
    private static final String optionsKeyword = "options";

    private static final String showKeyword = "show";
    private static final String unshowKeyword = "unshow";
    private static final String restartKeyword = "restart";
    private static final String descKeyword = "desc";
    private static final String pauseKeyword = "pause";
    private static final String resumeKeyword = "resume";
    private static final String activateKeyword = "activate";
    private static final String deactivateKeyword = "deactivate";

    private static final String selectKeyword = "select";
    private static final String fromKeyword = "from";
    private static final String whereKeyword = "where";
    private static final String topkKeyword = "topk";
    private static final String timeKeyword = "time";
    private static final String orKeyword = "or";
    private static final String andKeyword = "and";
    private static final String withinKeyword = "within";
    private static final String countKeyword = "count";

    public static String getIndexHashKeyword() {
        return indexHashKeyword;
    }

    public static String getIndexSpatialKeyword() {
        return indexSpatialKeyword;
    }

    static {
        MQLStartingKeywords =  new HashSet<>();
        MQLStartingKeywords.add(createKeyword);
        MQLStartingKeywords.add(dropKeyword);
        MQLStartingKeywords.add(showKeyword);
        MQLStartingKeywords.add(unshowKeyword);
        MQLStartingKeywords.add(selectKeyword);
        MQLStartingKeywords.add(restartKeyword);
        MQLStartingKeywords.add(descKeyword);
        MQLStartingKeywords.add(pauseKeyword);
        MQLStartingKeywords.add(resumeKeyword);
        MQLStartingKeywords.add(activateKeyword);
        MQLStartingKeywords.add(deactivateKeyword);

        MQLCreateStatKeywords = new HashSet<>();
        MQLCreateStatKeywords.add(streamKeyword);
        MQLCreateStatKeywords.add(indexKeyword);
        MQLCreateStatKeywords.add(indexHashKeyword);
        MQLCreateStatKeywords.add(indexSpatialKeyword);
        MQLCreateStatKeywords.add(onKeyword);
        MQLCreateStatKeywords.add(optionsKeyword);

        MQLSelectStatKeywords = new HashSet<>();
        MQLSelectStatKeywords.add(fromKeyword);
        MQLSelectStatKeywords.add(whereKeyword);
        MQLSelectStatKeywords.add(topkKeyword);
        MQLSelectStatKeywords.add(timeKeyword);
        MQLSelectStatKeywords.add(orKeyword);
        MQLSelectStatKeywords.add(andKeyword);
        MQLSelectStatKeywords.add(withinKeyword);
        MQLSelectStatKeywords.add(countKeyword);

        MQLAllKeywords = new HashSet<>();
        MQLAllKeywords.addAll(MQLStartingKeywords);
        MQLAllKeywords.addAll(MQLCreateStatKeywords);
        MQLAllKeywords.addAll(MQLSelectStatKeywords);
    }

    public static Pair<Pair<Boolean, String>,MetadataEntry> parseStatement
            (String statement) {
        statement = statement.trim();
        String originalStatement = statement;
        if(statement.isEmpty())
            return new Pair<> (new Pair<>(false,"Empty statement."),null);
        statement = statement.toLowerCase();

        String [] tokens = statement.split("\\s+");

        if(tokens.length > 0 && !MQLStartingKeywords.contains(tokens[0]))
            return new Pair<> (new Pair<>(false,"Syntax error. Invalid " +
                    "statement. No statement starts with "+tokens[0]),null);

        switch (tokens[0]) {
            case selectKeyword:
                return parseSelectStatement(tokens, statement);
            case showKeyword:
            case unshowKeyword:
                return parseShowStatement(tokens, statement);
            case pauseKeyword:
            case resumeKeyword:
                return parsePauseResumeStatement(tokens, statement);
            case activateKeyword:
            case deactivateKeyword:
                return parseActivateStatement(tokens, statement);
            case restartKeyword:
                return parseRestartStatement(tokens, statement);
            case createKeyword:
                return parseCreateStatement(tokens, statement);
            case dropKeyword:
                return parseDropStatement(tokens, statement);
            case descKeyword:
                return parseDescStatement(tokens, statement);
        }

        return new Pair<> (new Pair<>(false,"Syntax error in " +
                originalStatement), null);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry> parseSelectStatement
            (String[] tokens, String statement) {
        statement = statement.replaceAll(","," , ");
        statement = statement.replaceAll("\\s+"," ");

        String streamName = null;
        ArrayList<String> attributeNames = null;
        Query query = null;
        Hashtable<String, Object> queryParams = null;

        boolean successfulParsing = true;
        String errorMessage = null;
        MetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && tokens[i].compareTo(selectKeyword) == 0) {
            int whereInd = wholeWordIndexOf(whereKeyword, statement);
            int fromInd = wholeWordIndexOf(fromKeyword, statement);
            int selectInd = wholeWordIndexOf(selectKeyword, statement);
            int topkInd = wholeWordIndexOf(topkKeyword, statement);
            int timeInd = wholeWordIndexOf(timeKeyword, statement);

            if(selectInd < 0 || fromInd < 0) {
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid SELECT statement. " +
                        "Missing FROM clause.";
            } else if (fromInd < selectInd || (whereInd > 0 && whereInd <
                    fromInd) || (whereInd > 0 && topkInd > 0
                    && topkInd < whereInd) || (whereInd > 0 && timeInd > 0 &&
                    timeInd < whereInd) || (topkInd > 0 && topkInd < fromInd)
                    || (timeInd > 0 && timeInd < fromInd)) {
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid SELECT statement. " +
                        "The statement keywords right order is either " +
                        "SELECT FROM [WHERE] [TOPK] [TIME] or SELECT FROM " +
                        "[WHERE] [TIME] [TOPK].";
            } else {
                //extract clauses
                String selectClause = statement.substring(selectInd, fromInd);
                String fromClause = whereInd < 0? statement.substring(fromInd):
                        statement.substring(fromInd, whereInd);

                int topkStart = topkInd, topkEnd;
                int timeStart = timeInd, timeEnd;
                topkEnd = topkInd > timeInd? statement.length():timeInd;
                timeEnd = topkInd > timeInd? topkInd: statement.length();

                String topkClause = topkInd < 0? null: statement.substring
                        (topkStart, topkEnd);
                String timeClause = timeInd < 0? null: statement.substring
                        (timeStart, timeEnd);


                int whereStart = whereInd, whereEnd;
                int nextClauseStart;
                if(topkStart >= 0 && timeStart >= 0)
                    nextClauseStart = Math.min(topkStart, timeStart);
                else {
                    if (topkStart < 0) nextClauseStart = timeStart;
                    else nextClauseStart = topkStart;
                }

                whereEnd = nextClauseStart < 0? statement.length():
                        nextClauseStart;
                String whereClause = whereInd < 0? null: statement.substring
                        (whereStart, whereEnd);

                //parse SELECT clause
                String attributeList = selectClause.substring(selectKeyword
                        .length()+1);
                attributeList = attributeList.trim();
                if(attributeList.isEmpty()) {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid SELECT statement. " +
                            "Empty list of attributes.";
                } else {
                    attributeNames = new ArrayList<>();
                    if(attributeList.compareTo("*")==0) {
                        //mark later attributes expansion
                        attributeNames.add("*");
                    } else {
                        String [] attributeNamesStrs = attributeList.split
                                ("\\s*,\\s*");
                        for(String attributeName: attributeNamesStrs)
                            attributeNames.add(attributeName);
                    }
                    if(attributeNames.size() == 0) {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid SELECT " +
                                "statement. Empty list of attributes.";
                    }
                }

                //parse FROM clause
                if(successfulParsing) {
                    String[] fromTokens = fromClause.split("\\s+");
                    if (fromTokens.length > 1) {
                        streamName = fromTokens[1];

                        if (!KiteInstance.isExistingStream(streamName)) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid SELECT " +
                                    "statement. Stream in FROM clause does not " +
                                    "exist.";
                        } else {
                            if (attributeNames.get(0).compareTo("*") != 0) {
                                for (String attName : attributeNames) {
                                    if (!KiteInstance.isExistingAttribute
                                            (streamName, attName)) {
                                        successfulParsing = false;
                                        errorMessage = "Syntax error. Invalid" +
                                                " SELECT statement. " +
                                                "Attribute " + attName + " " +
                                                "does not exist in stream " +
                                                streamName + ".";
                                    }
                                }
                            }
                        }
                    } else{
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid SELECT statement. " +
                                "Missing stream name in FROM clause.";
                    }
                }

                //parse WHERE clause
                if(successfulParsing) {
                    if (whereClause == null) {
                        //compose temporal query
                        query = new Query(QueryType.PURE_TEMPORAL);
                    } else {
                        Pair<Pair<Boolean, String>, Query> whereResults =
                                parseWhereClause(whereClause, streamName);
                        successfulParsing = whereResults.getKey().getKey();
                        errorMessage = whereResults.getKey().getValue();
                        query = whereResults.getValue();
                    }
                }

                //parse TOPK clause
                if(successfulParsing) {
                    //parse topK clause
                    if(topkClause != null) {
                        String kStr = topkClause.substring(topkKeyword.length
                                ());
                        kStr = kStr.trim();
                        try {
                            int k = Integer.parseInt(kStr);
                            query.setK(k);
                        } catch (NumberFormatException e) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid SELECT " +
                                    "statement. Invalid k value in TOPK " +
                                    "clause.";
                        }
                    }
                }

                //parse TIME clause
                if(successfulParsing) {
                    //parse time
                    if(timeClause != null) {
                        String timeStr = timeClause.substring(timeKeyword
                                .length());
                        int openBracketIndex = timeStr.indexOf("[");
                        int closedBracketIndex = timeStr.indexOf("]");
                        int commaIndex = timeStr.indexOf(",");

                        if(openBracketIndex < 0 || closedBracketIndex < 0 ||
                                commaIndex < 0) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid SELECT " +
                                    "statement. Invalid TIME clause. Time " +
                                    "period must be in the form [startTime, " +
                                    "endTime].";
                        } else {
                            try {
                                String startTimeStr = timeStr.substring
                                        (openBracketIndex + 1, commaIndex);
                                startTimeStr = startTimeStr.trim();

                                String endTimeStr = timeStr.substring
                                        (commaIndex + 1, closedBracketIndex);
                                endTimeStr = endTimeStr.trim();

                                long startTime =  Date.parse (startTimeStr);
                                long endTime = Date.parse(endTimeStr);

                                TemporalPeriod period = new TemporalPeriod
                                        (startTime, endTime);
                                query.setTime(period);
                            } catch (IllegalArgumentException e) {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid SELECT " +
                                        "statement. Invalid TIME clause. " +
                                        "Invalid start time or end time " +
                                        "format.";
                            }
                        }
                    }
                }
            }
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }

        if(successfulParsing)
            metadata = new QueryMetadataEntry(streamName, attributeNames,
                    query);

        return new Pair<>(new Pair<>(successfulParsing, errorMessage), metadata);
    }

    private static Pair<Pair<Boolean, String>, Query> parseWhereClause
                                    (String whereClause, String streamName){
        //parse condition(s) and compose query
        //condition is: attribute=val AND (OR)
        // spatial_attribute WITHIN (n,s,e,w) AND (OR) ....
        // parentheses allowed, no nested parentheses

        boolean successfulParsing = true;
        String errorMessage = "";
        Condition condition = null;
        Query query = null;

        String conditionStr = whereClause.substring(whereKeyword
                .length());
        conditionStr = conditionStr.replaceAll("\\["," [ ");
        conditionStr = conditionStr.replaceAll("\\]"," ] ");
        conditionStr = conditionStr.replaceAll("\\("," ( ");
        conditionStr = conditionStr.replaceAll("\\)"," ) ");
        conditionStr = conditionStr.replaceAll(","," , ");
        conditionStr = conditionStr.replaceAll("="," = ");
        conditionStr = conditionStr.replaceAll("\\s+"," ");
        conditionStr = conditionStr.trim();

        int parenthesesValid = parenthesesValidity(conditionStr);
        if(parenthesesValid != 0) {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid SELECT statement. WHERE " +
                    "clause has mismatched or nested parentheses.";
        }

        if(successfulParsing) {
            //conditions
            String[] conditionTokens = conditionStr.split("\\s+");
            Pair<Pair<Boolean, String>, Condition> conditionResults =
                                parseCondition (conditionTokens, streamName);

            successfulParsing = conditionResults.getKey().getKey();
            errorMessage = conditionResults.getKey().getValue();
            condition = conditionResults.getValue();
        }


        if(successfulParsing) {
            //compose query
            query = new Query(QueryType.UNDECIDED, condition);
        }

        return new Pair<>(new Pair<>(successfulParsing, errorMessage), query);
    }

    private static Pair<Pair<Boolean,String>,Condition>
        parseCondition(String[] conditionTokens, String streamName) {

        boolean successfulParsing = true;
        String errorMessage = "";

        ArrayList<Object> flatCondition = new ArrayList<>();

        for(int tInd = 0; tInd < conditionTokens.length && successfulParsing;
            ++tInd) {
            if(conditionTokens[tInd].compareTo("=")==0) {
                if(tInd > 0 && tInd < conditionTokens.length-1) {
                    //parse attribute name and value
                    String attributeName = conditionTokens[tInd-1];
                    String attributeVal = conditionTokens[tInd+1];

                    Attribute attribute = KiteInstance.getAttribute(streamName,
                            attributeName);
                    if(attribute != null) {
                        SimpleCondition predicate = new SimpleCondition
                                (attribute, attributeVal);
                        flatCondition.add(predicate);
                    } else {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid SELECT statement."
                                +" Error in WHERE clause. " + attributeName+
                                " does not belong to stream "+streamName+".";
                    }
                    ++tInd;
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid  SELECT statement. " +
                            "Operator = in WHERE clause  must be preceeded by" +
                            " a valid attribute name and followed by a valid " +
                            "value.";
                }
            } else if(conditionTokens[tInd].compareTo(withinKeyword)==0) {
                if(tInd > 0 && tInd < conditionTokens.length-9) {
                    //parse attribute name and value
                    String attributeName = conditionTokens[tInd-1];
                    String openBracket = conditionTokens[tInd+1];
                    String closeBracket = conditionTokens[tInd+9];
                    if(openBracket.compareTo("[") != 0 || closeBracket
                            .compareTo("]") != 0) {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid SELECT " +
                                "statement. Error in WHERE clause. " +
                                "Invalid brackets after WITHIN.";
                    } else {
                        Attribute attribute = KiteInstance.getAttribute(streamName,
                                attributeName);
                        if(attribute != null && attribute.isSpatialAttribute()) {
                            Rectangle range = null;
                            boolean invalidCoordinates = false;
                            try {
                                range =new Rectangle(
                                        Double.parseDouble(conditionTokens[tInd+2]),
                                        Double.parseDouble(conditionTokens[tInd+4]),
                                        Double.parseDouble(conditionTokens[tInd+6]),
                                        Double.parseDouble(conditionTokens[tInd+8]));

                                invalidCoordinates =!(range.isBoundedCoordinates()
                                        && range.isValid());

                            } catch (NumberFormatException e) {
                                invalidCoordinates = true;
                            }
                            if(!invalidCoordinates) {
                                if (successfulParsing) {
                                    SimpleCondition spredicate = new SimpleCondition
                                            (attribute, range);
                                    flatCondition.add(spredicate);
                                }
                            } else {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid SELECT " +
                                        "statement. Error in WHERE clause. " +
                                        "Coordinates after WITHIN form invalid " +
                                        "spatial range.";
                            }
                        } else {
                            successfulParsing = false;
                            errorMessage = "Syntax error. " +
                                    "Invalid SELECT statement. Error in WHERE " +
                                    "clause. " + attributeName+" does not belong " +
                                    "to stream "+streamName+" or it is a " +
                                    "non-spatial attribute with a spatial " +
                                    "predicate.";
                        }
                    }
                    tInd += 9;
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid SELECT " +
                            "statement. Operator WITHIN " +
                            "in WHERE clause  must be " +
                            "preceeded by a valid " +
                            "spatial attribute name and " +
                            "followed by a valid value " +
                            "[north, south, east, west].";
                }
            } else if(conditionTokens[tInd].compareTo(orKeyword)==0) {
                flatCondition.add(orKeyword);
            } else if(conditionTokens[tInd].compareTo(andKeyword)==0) {
                flatCondition.add(andKeyword);
            } else if(conditionTokens[tInd].compareTo("(")==0) {
                //parse a full condition till ")"
                ArrayList<String> innerConditionStrs = new ArrayList<>();
                ++tInd;
                while(tInd < conditionTokens.length && conditionTokens[tInd]
                        .compareTo(")")!=0) {
                    innerConditionStrs.add(conditionTokens[tInd]);
                    ++tInd;
                }
                if(conditionTokens[tInd].compareTo(")")==0) {
                    String[] innerConditionTokens = new String[innerConditionStrs
                            .size()];
                    for(int ind = 0; ind < innerConditionStrs.size(); ++ind) {
                        innerConditionTokens[ind] = innerConditionStrs.get(ind);
                    }
                    Pair<Pair<Boolean,String>, Condition> innerConditionRes =
                            parseCondition (innerConditionTokens, streamName);
                    successfulParsing = innerConditionRes.getKey().getKey();
                    errorMessage = innerConditionRes.getKey().getValue();
                    Condition innerCondition = innerConditionRes.getValue();
                    flatCondition.add(innerCondition);
                } else {
                    errorMessage = "Syntax error. Invalid SELECT " +
                            "statement. Parenthesis mismatch in WHERE clause.";
                }
            }
        }
        if(conditionTokens.length <= 0) {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid SELECT statement. Empty " +
                    "condition in WHERE clause.";
        }
        Condition condition = null;
        if(successfulParsing) {
            Pair<Pair<Boolean, String>, Condition> flatConditionResult =
                    parseFlatCondition(flatCondition);
            successfulParsing = flatConditionResult.getKey().getKey();
            errorMessage = flatConditionResult.getKey().getValue();
            condition = flatConditionResult.getValue();
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                condition);
    }

    private static Pair<Pair<Boolean,String>,Condition> parseFlatCondition
            (ArrayList<Object> flatCondition) {
        boolean successfulParsing = true;
        String errorMessage = "";
        Condition condition = null;

        if(flatCondition.size() == 1) {
            try {
                condition = (Condition)flatCondition.get(0);
            } catch (ClassCastException e){
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid SELECT statement. " +
                        "Error parsing condition in WHERE clause.";
            }
        } else if(flatCondition.size() > 1) {
            String [] operators={andKeyword, orKeyword};//precedence-based order

            for(int opInd = 0; opInd < operators.length && successfulParsing;
                ++opInd) {
                int i = 1;
                while (i < flatCondition.size()) {
                    try {
                        String operator = (String) flatCondition.get(i);
                        if(operator.compareTo(operators[opInd]) == 0){
                            Condition leftCond = null;
                            Condition rightCond = null;
                            try {
                                leftCond = (Condition)flatCondition.get (i-1);
                                rightCond = (Condition)flatCondition.get (i+1);
                                Condition.LogicalOperator op =getOperator(operator);
                                CompoundCondition compCondition = new
                                        CompoundCondition(leftCond, op,
                                        rightCond);
                                //reduce the flat condition list
                                flatCondition.remove(i+1);
                                flatCondition.remove(i);
                                flatCondition.set(i-1,compCondition);
                            } catch (ClassCastException e) {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid SELECT " +
                                        "statement. Error parsing condition " +
                                        "in WHERE clause.";
                                break;
                            } catch (IndexOutOfBoundsException e) {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid SELECT " +
                                        "statement. Error parsing condition " +
                                        "in WHERE clause.";
                                break;
                            } catch (Exception e) {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid SELECT " +
                                        "statement. Error parsing condition " +
                                        "in WHERE clause.";
                                break;
                            }
                        } else
                            i += 2;
                    } catch (ClassCastException e) {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid SELECT statement."
                                +" Error parsing condition in WHERE clause.";
                        break;
                    }
                }
            }
            if(successfulParsing)
                condition = (Condition)flatCondition.get(0);
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid SELECT statement. " +
                    "Empty condition in WHERE clause.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                condition);
    }

    private static Condition.LogicalOperator getOperator(String operator) {
        switch (operator){
            case orKeyword:
                return Condition.LogicalOperator.OR;
            case andKeyword:
                return Condition.LogicalOperator.AND;
            default:
                return Condition.LogicalOperator.INVALID;
        }
    }

    private static int parenthesesValidity(String condition) {
        //check parentheses matching and no nested parentheses allowed
        //return value 0 means valid, otherwise shows an error
        int numOpenPara = 0;
        for(int i = 0; i < condition.length(); ++i) {
            char ch = condition.charAt(i);
            if (ch == '(')
                ++numOpenPara;
            if(ch == ')')
                --numOpenPara;
            if(numOpenPara > 1)
                return numOpenPara;
        }
        return numOpenPara;
    }

    private static int wholeWordIndexOf(String word, String str) {
        //assume word separators are only a single space character
        int ind = str.indexOf (word);
        while(ind > 0 && ind+word.length() < str.length() && str.charAt
                (ind+word.length()) != ' ') {
            int spaceInd = str.indexOf(' ', ind);
            ind = str.indexOf(word, spaceInd);
        }
        return ind;
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                    parseRestartStatement(String[] tokens, String statement) {
        String streamName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && tokens[i].compareTo(restartKeyword) == 0) {
            String command = tokens[i];
            ++i;//next token
            if (tokens.length > i) {
                if(KiteInstance.isExistingStream(tokens[i])) {
                    streamName = tokens[i];
                    metadata = new CommandMetadataEntry (command, streamName);
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid RESTART statement. " +
                            "Stream does not exist in the system.";
                }
            }
            else {
                successfulParsing = false;
                errorMessage = "Syntax error. Incomplete RESTART statement.";
            }
        }
        else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                    parseDropStatement(String[] tokens, String statement) {
        String streamName = null;
        String indexName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length >= 3 && tokens[i].compareTo(dropKeyword) == 0) {
            String command = tokens[i];
            ++i;//next token
            String dropType = tokens[i];
            ++i;//next token
            switch (dropType) {
                case indexKeyword:
                    indexName = tokens[i];
                    ++i;//next token
                    if(tokens.length > i) {
                        streamName = tokens[i];
                    } else {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid DROP " +
                                "INDEX statement. Stream name missing.";
                    }
                    break;
                case streamKeyword:
                    streamName = tokens[i];
                    break;
                default:
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid statement. The statement " +
                            "must be in the form of DROP STREAM stream_name or DROP " +
                            "INDEX index_name stream_name.";
                    break;
            }
            if(successfulParsing) {
                switch (dropType) {
                    case indexKeyword:
                        if (KiteInstance.isExistingIndex(streamName,indexName)){
                            metadata = new CommandMetadataEntry(command,
                                    dropType, indexName, streamName);
                        } else {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid DROP " +
                                    "statement. Stream and/or index do not " +
                                    "exist in the system.";
                        }
                        break;
                    case streamKeyword:
                        if (KiteInstance.isExistingStream(streamName)) {
                            metadata = new CommandMetadataEntry(command,
                                    dropType, streamName);
                        } else {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid DROP " +
                                    "statement. Stream does not exist in the " +
                                    "system.";
                        }
                        break;
                    default:
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid statement. The statement " +
                                "must be in the form of DROP STREAM stream_name or DROP " +
                                "INDEX index_name stream_name.";
                        break;
                }
            }
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement. The statement " +
                    "must be in the form of DROP STREAM stream_name or DROP " +
                    "INDEX index_name stream_name.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                        parseDescStatement(String[] tokens, String statement) {
        String streamName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && tokens[i].compareTo(descKeyword) == 0) {
            String command = tokens[i];
            ++i;//next token
            if (tokens.length > i) {
                if(KiteInstance.isExistingStream(tokens[i])) {
                    streamName = tokens[i];
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid DESC statement. " +
                            "Stream does not exist in the system.";
                }
            }
            else {
                streamName = "*";
            }
            if(successfulParsing)
                metadata = new CommandMetadataEntry (command, streamName);
        }
        else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                parseActivateStatement(String[] tokens, String statement) {
        String streamName = null;
        String indexName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && (tokens[i].compareTo(activateKeyword) == 0 ||
                tokens[i].compareTo(deactivateKeyword) == 0)) {
            String command = tokens[i];
            ++i;//next token
            if (tokens.length >= 3) {
                indexName = tokens[i];
                ++i;//next token
                streamName = tokens[i];
                if(KiteInstance.isExistingIndex(streamName, indexName)) {
                    metadata = new CommandMetadataEntry (command, indexName,
                            streamName);
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid ACTIVATE/DEACTIVATE "+
                            "statement. Index does not exist in the system.";
                }
            }
            else {
                successfulParsing = false;
                errorMessage = "Syntax error. Incomplete ACTIVATE/DEACTIVATE " +
                        "statement. The statment must be in the form of " +
                        "ACTIVATE/DEACTIVATE index_name stream_name.";
            }
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                parsePauseResumeStatement(String[] tokens, String statement) {
        String streamName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && (tokens[i].compareTo(pauseKeyword) == 0 ||
                tokens[i].compareTo(resumeKeyword) == 0)) {
            String command = tokens[i];
            ++i;//next token
            if (tokens.length > i) {
                if(KiteInstance.isExistingStream(tokens[i])) {
                    streamName = tokens[i];
                    metadata = new CommandMetadataEntry (command, streamName);
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid PAUSE/RESUME " +
                            "statement. Stream does not exist in the system.";
                }
            }
            else {
                successfulParsing = false;
                errorMessage = "Syntax error. Incomplete PAUSE/RESUME statement.";
            }
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
                        parseShowStatement(String[] tokens, String statement) {
        String streamName = null;

        boolean successfulParsing = true;
        String errorMessage = "";
        CommandMetadataEntry metadata = null;

        int i = 0;
        if(tokens.length > i && (tokens[i].compareTo(showKeyword) == 0 ||
                tokens[i].compareTo(unshowKeyword) == 0)) {
            String command = tokens[i];
            ++i;//next token
            if (tokens.length > i) {
                if(KiteInstance.isExistingStream(tokens[i])) {
                    streamName = tokens[i];
                    metadata = new CommandMetadataEntry (command, streamName);
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid SHOW/UNSHOW " +
                            "statement. Stream does not exist in the system.";
                }
            }
            else {
                successfulParsing = false;
                errorMessage = "Syntax error. Incomplete SHOW/UNSHOW statement.";
            }
        }
        else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid statement.";
        }
        return new Pair<>(new Pair<>(successfulParsing, errorMessage),
                metadata);
    }

    private static Pair<Pair<Boolean, String>,MetadataEntry>
                    parseCreateStatement(String[] tokens,
                                         String statementLowered) {
        int i = 0;
        if(tokens.length > i && tokens[i].compareTo(createKeyword) == 0) {
            ++i;//next token
            if (tokens.length > i && tokens[i].compareTo(streamKeyword) == 0) {
                //CREATE STREAM statement
                return parseCreateStreamStatement(tokens, statementLowered);
            } else if (tokens.length > i && tokens[i].compareTo(indexKeyword)
                    == 0) {
                return parseCreateIndexStatement(tokens, statementLowered);
            }
            else
                return new Pair<> (new Pair<> (false,"Syntax error. " +
                        "Incomplete CREATE statement or invalid asset type " +
                        "to create."),null);
        }
        else
            return new Pair<> (new Pair<> (false,"Syntax error. Invalid " +
                    "statement."), null);
    }

    private static Pair<Pair<Boolean,String>,MetadataEntry>
        parseCreateIndexStatement(String[] tokens, String statementLowered) {
        //CREATE INDEX statement

        boolean successfulParsing = true;
        String errorMessage = null;
        MetadataEntry metadata = null;

        //Index type
        int i = 2;//next token
        String indexType = null;
        if(tokens.length > i) {
            if(tokens[i].compareTo(indexHashKeyword) == 0) {
                indexType = indexHashKeyword;
            } else if(tokens[i].compareTo(indexSpatialKeyword) == 0) {
                indexType = indexSpatialKeyword;
            } else {
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid CREATE statement. " +
                        "Invalid index type.";
            }
        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid CREATE statement. " +
                    "Missing index type.";
        }

        if(indexType != null) {
            ++i; //next token
            switch (indexType) {
                case indexHashKeyword:
                    return parseCreateIndexClause(indexHashKeyword,
                            tokens, i, statementLowered);
                case indexSpatialKeyword:
                    String spatialPartitioningType =
                            parseSpatialPartitioningType(tokens,i);
                    if(spatialPartitioningType != null) {
                        ++i;
                        return parseCreateIndexClause(
                                indexSpatialKeyword + spatialPartitioningType,
                                tokens, i, statementLowered);
                    } else {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid CREATE statement. " +
                                "Invalid spatial index type.";
                        return new Pair<>(new
                                Pair<>(successfulParsing, errorMessage), metadata);
                    }
                default:
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid CREATE statement. " +
                            "Invalid index type.";
                    return new Pair<>(new
                            Pair<>(successfulParsing, errorMessage), metadata);
            }
        }

        Pair<Pair<Boolean,String>,MetadataEntry> result = new Pair<>(new
                Pair<>(successfulParsing, errorMessage), metadata);
        return result;
    }

    private static String parseSpatialPartitioningType(String[] tokens, int
            ind) {
        if(ind >= tokens.length || !SpatialPartitioner.isValidType(tokens[ind]))
            return null;
        else
            return tokens[ind];
    }

    private static Pair<Pair<Boolean, String>, MetadataEntry>
    parseCreateIndexClause (String indexTyp, String [] tokens, int
            nextTokenInd, String statementLowered) {
        //Clause format:
        //index_name ON stream_name (att_name)
        String indexType = indexTyp;
        String indexName = null;
        String streamName = null;
        String attributeName = null;
        List<Object> options = null;

        boolean successfulParsing = true;
        String errorMessage = null;
        MetadataEntry metadata = null;

        int i = nextTokenInd; //next token
        if(tokens.length > i &&
                idWtUnderscorePattern.matcher(tokens[i]).matches()) {
            indexName = tokens[i];

        } else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid CREATE statement. " +
                    "Missing or invalid index name. Index name must " +
                    "include only letters and/or digits.";
        }

        ++i;//next token
        if(!(tokens.length > i && tokens[i].compareTo(onKeyword) == 0)) {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid CREATE statement. " +
                    "Missing ON keyword after index name.";
        }

        if(successfulParsing) {
            String tmpStreamName, tmpAttributeName;
            String rest;

            int onInd = statementLowered.indexOf(onKeyword);
            int optionsInd = statementLowered.indexOf(optionsKeyword);

            if(optionsInd > 0 && optionsInd < onInd) {
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid CREATE statement. The " +
                        "statement keywords right order is CREATE INDEX ON " +
                        "[OPTIONS].";
            } else {
                String streamAttNames = optionsInd < 0 ? statementLowered.substring
                        (onInd + onKeyword.length() + 1) :
                        statementLowered.substring(onInd + onKeyword.length() + 1,
                                optionsInd);
                String[] names = streamAttNames.split("\\s*\\(\\s*|\\s*\\)\\s*");
                if (names.length > 1) {
                    tmpStreamName = names[0];
                    tmpAttributeName = names[1];

                    if (KiteInstance.isExistingAttribute(tmpStreamName,
                            tmpAttributeName)) {
                        streamName = tmpStreamName;
                        attributeName = tmpAttributeName;

                        if (KiteInstance.isExistingIndex(streamName, indexName)) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid CREATE statement."
                                    + " Index " + indexName + " already exists on " +
                                    "stream " + streamName + ".";
                        }

                        if (successfulParsing && !KiteInstance.isHomeNode(streamName)) {
                            successfulParsing = false;
                            errorMessage = "Administration error. Invalid CREATE " +
                                    "statement. Index " + indexName + " must be " +
                                    "created on the home node of stream "
                                    + streamName + ".";
                        }
                        //parse options (if exists)
                        if (optionsInd > 0 && successfulParsing) {
                            rest = statementLowered.substring
                                    (optionsInd + optionsKeyword.length());
                            rest = rest.trim();
                            String[] optionsStrs = rest.split("\\s*,\\s*");
                            if (optionsStrs.length > 0) {
                                int intdexCapacity = ConstantsAndDefaults.
                                        TEMPORAL_FLUSHING_PERIODIC_DATA_SIZE;
                                try {
                                    if (optionsStrs[0].compareTo("default") != 0) {
                                        intdexCapacity = Integer.parseInt(
                                                optionsStrs[0]);
                                    }
                                } catch (NumberFormatException e) {
                                    successfulParsing = false;
                                    errorMessage = "Syntax error. Invalid CREATE " +
                                            "statement. Invalid OPTIONS clause.";
                                }

                                int indexSegments = ConstantsAndDefaults.
                                        MEMORY_INDEX_DEFAULT_SEGMENTS;
                                if (optionsStrs.length > 1) {
                                    try {
                                        if (optionsStrs[1].compareTo("default") != 0) {
                                            indexSegments = Integer.parseInt(
                                                    optionsStrs[1]);
                                        }
                                    } catch (NumberFormatException e) {
                                        successfulParsing = false;
                                        errorMessage = "Syntax error. Invalid CREATE " +
                                                "statement. Invalid OPTIONS clause.";
                                    }
                                }

                                options = new ArrayList<>();
                                if (successfulParsing) {
                                    options.add(intdexCapacity);
                                    options.add(indexSegments);
                                }
                                if (successfulParsing && indexType.startsWith
                                        (indexSpatialKeyword)) {
                                /* parameters of {@link GridPartitioner#GridPartitioner(Rectangle, int, int)}
                                 *
                                 */
                                    int tmpCounter = 0;
                                    for (int ind = 2; tmpCounter < 6; ++ind,
                                            ++tmpCounter) {
                                        if (ind < optionsStrs.length) {
                                            try {
                                                if (optionsStrs[ind].compareTo("default")
                                                        == 0) {
                                                    //get default values
                                                    options.add
                                                            (indexOptionsDefaultValues[ind]);
                                                } else if (tmpCounter < 4) {
                                                    options.add(Double.parseDouble
                                                            (optionsStrs[ind]));
                                                } else
                                                    options.add(Integer.parseInt
                                                            (optionsStrs[ind]));
                                            } catch (NumberFormatException e) {
                                                successfulParsing = false;
                                                errorMessage = "Syntax error. " +
                                                        "Invalid CREATE statement" +
                                                        ". Invalid OPTIONS clause.";
                                            }
                                        } else
                                            options.add(indexOptionsDefaultValues[ind]);
                                    }
                                }
                            }
                        } else {
                            if (names.length > 2) {
                                successfulParsing = false;
                                errorMessage = "Syntax error. Invalid CREATE " +
                                        "statement. Invalid tokens after " +
                                        "attribute name.";
                            }
                        }
                    } else {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid CREATE statement. "
                                + "Stream or attribute does not exist in the " +
                                "system.";
                    }
                } else {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid CREATE statement. "
                            + "Missing stream and/or attribute names.";
                }
            }
        }

        if(successfulParsing)
            metadata = new IndexMetadataEntry(indexType, indexName,
                    streamName, attributeName, options);
        return new Pair<>(new Pair<>(successfulParsing,errorMessage), metadata);
    }

    private static Pair<Pair<Boolean, String>,MetadataEntry>
                parseCreateStreamStatement(String[] tokens,
                                           String statementLowered) {
        boolean successfulParsing = true;
        String errorMessage = null;
        MetadataEntry metadata = null;

        String streamName = null;
        ArrayList<Attribute> attributes = null;
        StreamSourceInfo source = null;
        StreamFormatInfo format = null;

        //Stream name
        int i = 2;//next token
        if(tokens.length > i) {
            String tmpName = tokens[i];
            if(tokens[i].contains("("))
                tmpName = tokens[i].substring(0,tokens[i].indexOf('('));
            if(idWtUnderscorePattern.matcher(tmpName).matches() &&
                    !MQLAllKeywords.contains(tmpName)) {
                //valid stream name
                streamName = tmpName;
                if(KiteInstance.isExistingStream(streamName)) {
                    successfulParsing = false;
                    errorMessage = "Syntax error. Invalid CREATE statement. " +
                            "Stream "+streamName+" already exists in the " +
                            "system.";
                    streamName = null;
                }
            } else {
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid CREATE statement. " +
                        "Missing or invalid stream name. Stream name must " +
                        "include only letters and/or digits and cannot be a " +
                        "reserved keyword.";
            }
        }
        else {
            successfulParsing = false;
            errorMessage = "Syntax error. Invalid CREATE statement. " +
                    "Missing or invalid stream name. Stream name must " +
                    "include only letters and/or digits.";
        }

        if(streamName != null) {////Stream name is valid, continue parsing
            //get 3 parts: attribute list, FROM clause, and FORMAT clause
            int indLftParanth = statementLowered.indexOf('(');
            int indRigtParanth = statementLowered.indexOf(')');
            int indFROM = statementLowered.indexOf("from");
            int indFORMAT = statementLowered.indexOf("format");

            if(indLftParanth < 0 || indRigtParanth < 0 || indFROM < 0 ||
                    indFORMAT < 0 || indLftParanth > indRigtParanth ||
                    indFROM > indFORMAT || indRigtParanth > indFROM ||
                    indLftParanth > indFROM) {//invalid statement
                successfulParsing = false;
                errorMessage = "Syntax error. Invalid CREATE statement. " +
                        "Missing or invalid stream attribute list, source, or" +
                        " format.";
            }
            else {
                String attributeList = statementLowered.substring
                        (indLftParanth+1,indRigtParanth);
                String fromClause = statementLowered.substring(indFROM,
                        indFORMAT);
                String formatClause = statementLowered.substring(indFORMAT);

                //parse attribute list
                Pair<ArrayList<Attribute>, String> attributeParseResults =
                        parseCreateStreamAttributeList(attributeList);
                attributes = attributeParseResults.getKey();

                //parse FROM clause
                if(attributes != null)//if attributes parsed, continue parsing
                {
                    source = parseCreateStreamFromClause(fromClause);

                    //parse FORMAT clause
                    if(source != null) {//if attributes parsed, continue parsing
                        format = parseCreateStreamFormatClause(formatClause);
                        if(format == null) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid CREATE " +
                                    "statement. Invalid stream format.";
                        }
                        else if (!format.isCompatible(attributes)) {
                            successfulParsing = false;
                            errorMessage = "Syntax error. Invalid CREATE " +
                                    "statement. Stream format incompatible with " +
                                    "attribute list.";
                        }
                    } else {
                        successfulParsing = false;
                        errorMessage = "Syntax error. Invalid CREATE " +
                                "statement. Invalid stream source.";
                    }
                }
                else {
                    successfulParsing = false;
                    errorMessage = attributeParseResults.getValue();
                }
            }
        }
        if(successfulParsing)
            metadata = new StreamMetadataEntry(streamName, attributes, source,
                    format);
        return new Pair<>(new Pair<>(successfulParsing, errorMessage), metadata);
    }

    private static Pair<ArrayList<Attribute>, String>
                     parseCreateStreamAttributeList (String attributeList) {
        ArrayList<Attribute> attributes = new ArrayList<>();
        String errorMessage = "";

        String [] attributesStrs = attributeList.split("\\s*,\\s*");

        for(int j = 0; j < attributesStrs.length && attributes != null; ++j) {
            int indColon = attributesStrs[j].indexOf(':');
            if(indColon <= 0) {
                attributes = null;
                errorMessage = "Syntax error. Invalid CREATE " +
                        "statement. Invalid attribute list. Attribute " +
                        "declaration must be in the form of " +
                        "attribute_name:data_type.";
            } else {
                String attributeName = attributesStrs[j].substring(0,
                        indColon);
                String attributeType = attributesStrs[j].substring(
                        indColon+1);
                if(!idPattern.matcher(attributeName).matches() ||
                        !Attribute.isValidDataType(attributeType) ||
                        MQLAllKeywords.contains(attributeName)) {
                    attributes = null;
                    errorMessage = "Syntax error. Invalid CREATE " +
                            "statement. Invalid attribute name or type in "+
                            attributeName+":"+attributeType+". Attribute name" +
                            " must consists of only letters, digits, and/or " +
                            "underscores, and cannot be a reserved keyword.";
                } else {
                    attributes.add(new Attribute(attributeName,attributeType));
                }
            }
        }
        if(attributes != null && attributes.size() == 0) {
            attributes = null;
            errorMessage = "Syntax error. Invalid CREATE statement. Empty " +
                    "list of attributes.";
        }
        return new Pair<>(attributes, errorMessage);
    }

    private static StreamFormatInfo parseCreateStreamFormatClause(String
                                    formatClause) {
        int indLftParanth = formatClause.indexOf('(');
        int indRigtParanth = formatClause.indexOf(')');
        if(indLftParanth < 0 || indRigtParanth < 0 || indRigtParanth <
                indLftParanth)
            return null;
        String formatType = formatClause.substring(7,indLftParanth);
        String formatList = formatClause.substring(indLftParanth+1,
                indRigtParanth);

        formatType = formatType.trim();
        formatList = formatList.trim();
        if(StreamFormatInfo.isValidStreamFormat(formatType)) {
            return StreamFormatInfo.getStreamFormatInfo(formatType, formatList);
        } else return null;
    }

    private static StreamSourceInfo parseCreateStreamFromClause(String
                                                       fromClause) {
        int indLftParanth = fromClause.indexOf('(');
        int indRigtParanth = fromClause.indexOf(')');
        if(indLftParanth < 0 || indRigtParanth < 0 || indRigtParanth <
                indLftParanth)
            return null;
        String sourceType = fromClause.substring(5,indLftParanth);
        String sourceCredentials = fromClause.substring(indLftParanth+1,
                indRigtParanth);

        sourceType = sourceType.trim();
        sourceCredentials = sourceCredentials.trim();
        if(StreamSourceInfo.isValidStreamSource(sourceType)) {
            return StreamSourceInfo.getStreamSourceInfo(sourceType , sourceCredentials);
        } else return null;
    }

    public static Pair<Pair<Boolean, String>, MQLResults> executeStatement(
            Pair<Pair<Boolean, String>, MetadataEntry> parsingResults) {
        Pair<Pair<Boolean, String>, MQLResults> executionResults = null;

        Boolean successfulParsing = parsingResults.getKey().getKey();
        String errorMessage = parsingResults.getKey().getValue();
        MetadataEntry metaData = parsingResults.getValue();

        if(successfulParsing) {
            if(metaData.isQueryEntry()) {
                executionResults = executeQuery((QueryMetadataEntry)
                        metaData);
                boolean successfulExecution = executionResults.getKey()
                        .getKey();
                String executionErrorMessage = executionResults.getKey()
                        .getValue();
                MQLResults results = executionResults.getValue();
                if(successfulExecution) {
                    String msg1 = "Query executed successfully on stream "+
                            ((QueryMetadataEntry)metaData).getStreamName();
                    System.out.println("Query executed successfully!");
                    results.print();
                    String msg2 = results.getAnswerSize()+" records " +
                            "matched!";
                    System.out.println(msg2);
                    KiteInstance.logOut(msg1+", "+msg2);
                }
                else {
                    System.err.println(executionErrorMessage);
                    KiteInstance.logError(executionErrorMessage);
                }
            } else if(metaData.isCommandEntry()) {
                CommandMetadataEntry commandEntry = (CommandMetadataEntry)
                        metaData;
                executionResults = executeCommand(commandEntry);

                boolean successfulExecution = executionResults.getKey()
                        .getKey();
                String executionErrorMessage = executionResults.getKey()
                        .getValue();
                MQLResults results = executionResults.getValue();
                if(successfulExecution) {
                    String msg = commandEntry.getCommand().toUpperCase() +" " +
                            "command executed successfully on "+commandEntry
                            .getArgsString()+"!";
                    System.out.println(msg);
                    KiteInstance.logOut(msg);
                }
                else {
                    System.err.println(executionErrorMessage);
                    KiteInstance.logError(executionErrorMessage);
                }

            } else if(metaData.isIndexEntry()) {
                IndexMetadataEntry indexMetadata = (IndexMetadataEntry)metaData;
                executionResults = executeIndexingRequest(indexMetadata, true);
                boolean successfulExecution = executionResults.getKey()
                        .getKey();
                String executionErrorMessage = executionResults.getKey()
                        .getValue();
                MQLResults results = executionResults.getValue();
                if(successfulExecution) {
                    String msg = indexMetadata.getIndexName()+" " +
                            "index successfully created on "+indexMetadata
                            .getStreamName()+":"+indexMetadata
                            .getAttributeName()+"!";
                    System.out.println(msg);
                    KiteInstance.logOut(msg);
                }
                else {
                    System.err.println(executionErrorMessage);
                    KiteInstance.logError(executionErrorMessage);
                }
            } else if(metaData.isStreamEntry()) {
                StreamMetadataEntry streamMetadata = (StreamMetadataEntry)
                        metaData;
                executionResults = executeStreamCreation(streamMetadata, true);
                boolean successfulExecution = executionResults.getKey()
                        .getKey();
                String executionErrorMessage = executionResults.getKey()
                        .getValue();
                MQLResults results = executionResults.getValue();
                if(successfulExecution) {
                    String msg = "Stream "+streamMetadata.getStreamName() +
                            " connection executed successfully!";
                    System.out.println(msg);
                    KiteInstance.logOut(msg);
                }
                else {
                    System.err.println(executionErrorMessage);
                    KiteInstance.logError(executionErrorMessage);
                }
            }
        }
        else {
            System.err.println(errorMessage);
            KiteInstance.logError(errorMessage);
        }
        return executionResults;
    }

    private static Pair<Pair<Boolean,String>,MQLResults> executeQuery(
            QueryMetadataEntry queryMetadata) {
        StreamDataset stream = KiteInstance.getStream (queryMetadata
                .getStreamName());

        boolean successfulExecution = true;
        String errorMessage = "";
        MQLResults results = null;

        if(stream != null) {
            results = stream.search (queryMetadata.getQuery(), queryMetadata
                    .getAttributeNames());
            if(results == null) {
                successfulExecution = false;
                errorMessage = "Query is not successfully executed!";
            }
        }
        else {
            successfulExecution = false;
            errorMessage = "Administration error. Stream "+
                    queryMetadata.getStreamName()+" is not active on this " +
                    "node.";
        }
        return new Pair<>(new Pair<>(successfulExecution, errorMessage),
                results);
    }

    public static Pair<Pair<Boolean,String>, MQLResults> executeStreamCreation
            (StreamMetadataEntry streamMetadata, boolean addToMetadata) {
        boolean successfulExecution = true;
        String errorMessage = "";

        StreamDataset stream = new StreamDataset
                (streamMetadata.getStreamName(),
                        StreamingDataSource.create(
                                streamMetadata.getStreamSourceInfo(),
                                streamMetadata.getScheme(),
                                streamMetadata.getStreamFormatInfo()));
        KiteInstance.addStream(stream.getName(), stream);

        int counter = 0;
        boolean metadataAdded;
        do {
            if(addToMetadata)
                metadataAdded = KiteInstance.addMetadata(streamMetadata);
            else
                metadataAdded = true;
            if(!metadataAdded) {
                successfulExecution = false;
                errorMessage = "Stream " + stream.getName() + " metadata failed " +
                        "to be added. Check file system settings.";
            } else {
                successfulExecution = true;
                errorMessage = "";
            }
            ++counter;
        } while (!metadataAdded && counter < 3);
        if(metadataAdded) {
            String msg = "Stream " + stream.getName() + " successfully " +
                    "created!";
            System.out.println(msg);
            KiteInstance.logOut(msg);
            stream.startStreaming();
        }
        else {
            System.err.println(errorMessage);
            //removing the stream from the system
            KiteInstance.removeStream(stream.getName());
            KiteInstance.logError(errorMessage);
        }

        return new Pair<>(new Pair<>(successfulExecution, errorMessage), null);
    }

    public static Pair<Pair<Boolean, String>, MQLResults>
                    executeIndexingRequest (IndexMetadataEntry indexMetadata,
                     boolean addToMetadata) {
        boolean successfulExecution = false;
        String errorMessage = "";

        StreamDataset stream = KiteInstance.getStream(indexMetadata
                .getStreamName());
        List<Object> options = indexMetadata.getOptions();
        Attribute attribute = stream.getAttribute(
                indexMetadata.getAttributeName());

        String indexType = "";
        switch (indexMetadata.getIndexType()) {
            case "hash":
                if(options == null)
                    stream.createIndexHash(attribute, indexMetadata
                        .getIndexName(), !addToMetadata);
                else
                    stream.createIndexHash(attribute, indexMetadata
                            .getIndexName(), (Integer)options.get(0),
                            (Integer)options.get(1), !addToMetadata);
                indexType = "Hash";
                break;
            case "spatialgrid":
                if(options == null)
                    stream.createIndexSpatial(attribute,
                            indexMetadata.getIndexName(), new
                                    GridPartitioner(new Rectangle
                                    ((Double) indexOptionsDefaultValues[2],
                                     (Double) indexOptionsDefaultValues[3],
                                     (Double) indexOptionsDefaultValues[4],
                                     (Double) indexOptionsDefaultValues[5]),
                                     (Integer)indexOptionsDefaultValues[6],
                                     (Integer)indexOptionsDefaultValues[7]),
                            !addToMetadata);
                else
                    stream.createIndexSpatial(attribute,
                            indexMetadata.getIndexName(), new
                                    GridPartitioner(new Rectangle
                                    ((Double) options.get(2),
                                            (Double) options.get(3),
                                            (Double) options.get(4),
                                            (Double) options.get(5)),
                                    (Integer)options.get(6),
                                    (Integer)options.get(7)),(Integer)options
                                    .get(0), (Integer)options.get(1),
                                    !addToMetadata);
                indexType = "Spatial grid";
                break;
        }

        boolean metadataAdded;
        int counter = 0;
        do {
            if(addToMetadata)
                metadataAdded = KiteInstance.addMetadata(indexMetadata);
            else
                metadataAdded = true;
            if(!metadataAdded) {
                successfulExecution = false;
                errorMessage = indexType+" index "+ indexMetadata.getIndexName()
                        +" metadata failed to be added. Check file system " +
                        "settings.";
            }  else {
                successfulExecution = true;
                errorMessage = "";
            }
            ++counter;
        } while (!metadataAdded && counter < 3);
        if(metadataAdded) {
            if (stream.activateIndex(indexMetadata.getIndexName())) {
                String msg = indexType + " index " + indexMetadata
                        .getIndexName() + " successfully created on " + stream
                        .getName() + ":" + attribute.getName();
                System.out.println(msg);
                KiteInstance.logOut(msg);
            } else {
                successfulExecution = false;
                errorMessage = indexType + " index " + indexMetadata.getIndexName()
                        + " failed to be activated. Please perform the " +
                        "activation operation manually with ACTIVATE " +
                        "statement (or equivalent).";
            }
        } else {
            KiteInstance.logError(errorMessage);
            System.err.println(errorMessage);
            stream.destroyIndex(indexMetadata.getIndexName());
        }
        return new Pair<>(new Pair<>(successfulExecution, errorMessage), null);
    }

    private static Pair<Pair<Boolean, String>, MQLResults> executeCommand
            (CommandMetadataEntry command) {
        boolean successfulExecution = true;
        String errorMessage = "";

        switch (command.getCommand()) {
            case dropKeyword:
                String dropType = (String) command.getArg(0);
                if(dropType.compareTo(indexKeyword)==0)
                    return executeDropIndex((String) command.getArg(1),
                            (String)command.getArg(2));
                else if(dropType.compareTo(streamKeyword)==0)
                    return executeDropStream((String) command.getArg(1));
                break;

            case restartKeyword:
                String streamName = (String) command.getArg(0);
                if(!KiteInstance.restartStream(streamName)) {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " does not exist in the system.";
                } else {
                    if(!KiteInstance.restartStreamIndexes(streamName)) {
                        successfulExecution = false;
                        errorMessage = "Administration error. One or " +
                                "more of stream "+ streamName +
                                " indexes failed to restart.";
                    }
                }
                break;
            case descKeyword:
                streamName = (String) command.getArg(0);
                if(streamName.compareTo("*")==0)
                    KiteInstance.descAllStreams ();
                else
                    KiteInstance.descStream (streamName);
                break;
            case pauseKeyword:
                streamName = (String) command.getArg(0);
                StreamDataset stream = KiteInstance.getStream (streamName);
                if(stream != null)
                    stream.stop();
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            case resumeKeyword:
                streamName = (String) command.getArg(0);
                stream = KiteInstance.getStream (streamName);
                if(stream != null)
                    stream.resume();
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            case activateKeyword:
                String indexName = (String) command.getArg(0);
                streamName = (String) command.getArg(1);
                stream = KiteInstance.getStream (streamName);
                if(stream != null) {
                    successfulExecution = stream.activateIndex(indexName);
                    if(!successfulExecution)
                        errorMessage = "Administration error. Unable to " +
                                "activate index " + indexName + ".";
                }
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            case deactivateKeyword:
                indexName = (String) command.getArg(0);
                streamName = (String) command.getArg(1);
                stream = KiteInstance.getStream (streamName);
                if(stream != null) {
                    successfulExecution = stream.deactivateIndex(indexName);
                    if(!successfulExecution)
                        errorMessage = "Administration error. Unable to " +
                                "deactivate index " + indexName + ".";
                }
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            case showKeyword:
                streamName = (String) command.getArg(0);
                stream = KiteInstance.getStream (streamName);
                if(stream != null)
                    stream.show();
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            case unshowKeyword:
                streamName = (String) command.getArg(0);
                stream = KiteInstance.getStream(streamName);
                if(stream != null)
                    stream.unshow();
                else {
                    successfulExecution = false;
                    errorMessage = "Administration error. Stream "+ streamName +
                            " is not active on this node.";
                }
                break;
            default:;
        }
        return new Pair<>(new Pair<>(successfulExecution, errorMessage), null);
    }

    private static Pair<Pair<Boolean,String>,MQLResults> executeDropIndex
            (String indexName, String streamName) {
        boolean successfulExecution = true;
        String errorMessage = "";

        StreamDataset stream = KiteInstance.getStream(streamName);
        if(stream != null) {
            boolean removedMetadata = KiteInstance.removeIndexMetadata
                    (streamName,
                    indexName) ;
            boolean destroyedIndex = false;
            if(removedMetadata) {
                destroyedIndex = stream.destroyIndex(indexName);
                //if(!destroyedIndex)//add index meta data again
                  //stream.getIndexMetadata(indexName);
                  //Currently, not destroyed means does not exist in this stream

            }
            successfulExecution = removedMetadata && destroyedIndex;
            if(!successfulExecution) {
                errorMessage = "Administration error. Failed to drop index "
                        + indexName+ ".";
            }
        } else {
            successfulExecution = false;
            errorMessage = "Administration error. Stream "+ streamName +
                    " is not active on this node.";
        }
        return new Pair<>(new Pair<>(successfulExecution, errorMessage), null);
    }

    private static Pair<Pair<Boolean,String>,MQLResults> executeDropStream
            (String streamName) {
        boolean successfulExecution = true;
        String errorMessage = "";

        StreamDataset stream = KiteInstance.getStream(streamName);
        if(stream != null) {
            stream.stop();
            successfulExecution = stream.destroyAllIndexes();
            successfulExecution = KiteInstance.removeStreamMetadata(streamName)
                    && successfulExecution;
            if(successfulExecution)
                KiteInstance.removeStream(streamName);
            else {
                errorMessage = "Administration error. Failed to drop stream "
                        + streamName+ ".";
            }
        } else {
            successfulExecution = false;
            errorMessage = "Administration error. Stream "+ streamName +
                    " is not active on this node. Stream must be dropped on " +
                    "its home node.";
        }
        return new Pair<>(new Pair<>(successfulExecution, errorMessage), null);
    }
}
