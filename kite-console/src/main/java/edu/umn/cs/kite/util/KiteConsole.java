package edu.umn.cs.kite.util;

import edu.umn.cs.kite.common.KiteInstance;
import edu.umn.cs.kite.common.KiteLaunchTool;
import edu.umn.cs.kite.querying.MQL;
import edu.umn.cs.kite.querying.metadata.MetadataEntry;
import javafx.util.Pair;

/**
 * Created by amr_000 on 8/7/2016.
 */
public class KiteConsole {
    public static void main(String [] args) {
        String settingsFile = null;
        if(args.length > 0)
            settingsFile = args[0];
        KiteConsole.run(settingsFile);
    }

    private static void run(String settingsFile) {
        try {
            startKiteInstance(settingsFile);
            String startMsg = "New Kite node " + KiteInstance.getLocalNodeName() +
                    " started ... " + KiteInstance.getNodesCount() + " node(s) " +
                    "currently participating in Kite cluster";
            KiteInstance.printMessageOnAllClusterNodes(startMsg);
        } catch (Exception e) {
            String errMsg = "Accidental error!!";
            errMsg += System.lineSeparator();
            errMsg += "Error: " + e.getMessage();
            KiteInstance.logError(errMsg);
            System.err.println(errMsg);
            System.out.println("The system operation is continuing.");
        }

        do {
            try {
                boolean exit;
                TimerExecution executor = new TimerExecution();
                do {
                    String statement = System.console().readLine();
                    exit = statement == null || statement.toLowerCase
                            ().compareTo("exit") == 0 || statement
                            .toLowerCase().compareTo("quit") == 0;

                    if (!exit) {
                        System.out.println("Parsing \"" + statement + "\"");

                        Pair<Pair<Boolean, String>, MetadataEntry> parsingResults
                                = MQL.parseStatement(statement);
                        boolean done = executor.executeQuery(parsingResults,
                                30000);
                        if(!done) {
                            String errMsg = "An MQL statement has encountered" +
                                    " either too long time to execute or " +
                                    "accidental error. The execution of " +
                                    "this statement has been terminated " +
                                    "abnormally.";
                            System.err.println(errMsg);
                            KiteInstance.logError(errMsg);
                        }
                    }
                } while (!exit);

                KiteInstance.closeGracefully(0);
            } catch (OutOfMemoryError e) {
                String errMsg = "All active streams will be paused. " +
                        "Accidental out of memory error.";
                errMsg += System.lineSeparator();
                errMsg += "Error: " + e.getMessage();
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
                KiteInstance.pauseAllActiveStreams();
                System.out.println("The system operation is continuing.");
            } catch (Exception e) {
                String errMsg = "An MQL statement has encountered " +
                        "accidental error. The execution of this statement " +
                        "has been terminated abnormally.";
                KiteInstance.logError(errMsg);
                System.err.println(errMsg);
                System.out.println("The system operation is continuing.");
            }
        } while (true);
    }

    private static void startKiteInstance(String settingsFile) {
        KiteLaunchTool kite = new KiteLaunchTool();
        if(settingsFile == null)
            KiteInstance.initSettings(kite);
        else
            KiteInstance.initSettings(kite, settingsFile);
    }
}
