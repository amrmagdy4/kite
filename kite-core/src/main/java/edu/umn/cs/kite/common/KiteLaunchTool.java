package edu.umn.cs.kite.common;

/**
 * Created by amr_000 on 8/1/2016.
 */
import edu.umn.cs.kite.util.ConstantsAndDefaults;
import edu.umn.cs.kite.util.GC;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Timer;
import java.util.TimerTask;

public class KiteLaunchTool {
    private Ignite cluster = null;
    private String configurationXMLFile = null;
    private Timer gcTimer;

    public Ignite getCluster() { return cluster; }

    public KiteLaunchTool(String configurationXML) {
        cluster = Ignition.start(configurationXML);
        init();
    }
    public KiteLaunchTool() {
        boolean peerClassLoading = true;
        IgniteConfiguration config = new IgniteConfiguration();
        config.setPeerClassLoadingEnabled(peerClassLoading);
        cluster = Ignition.start(config);
        init();
    }

    private void init(){
        printKiteName();
        startGCTimer();
    }

    private void startGCTimer() {
        gcTimer = new Timer(true);
        long timerPeriod = ConstantsAndDefaults.GC_PERIOD_MILLISECONDS;
        gcTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                GC.invoke();
            }
        }, timerPeriod, timerPeriod);
    }
    private void printKiteName() {

        System.out.println("KKKKKKKKK    KKKKKKK     iiii             tttt");
        System.out.println("K:::::::K    K:::::K    i::::i         ttt:::t");
        System.out.println("K:::::::K    K:::::K     iiii          t:::::t");
        System.out.println("K:::::::K   K::::::K                   t:::::t");
        System.out.println("KK::::::K  K:::::KKK   iiiiiii   " +
                "ttttttt:::::ttttttt           eeeeeeeeeeee");
        System.out.println("K:::::K K:::::K      i:::::i   " +
                "t:::::::::::::::::t         ee::::::::::::ee");
        System.out.println("K::::::K:::::K        i::::i   " +
                "t:::::::::::::::::t        e::::::eeeee:::::ee");
        System.out.println("K:::::::::::K         i::::i  tttttt:::::::tttttt" +
                "       e::::::e     e:::::e");
        System.out.println("K:::::::::::K         i::::i         t:::::t     " +
                "        e:::::::eeeee::::::e");
        System.out.println("K::::::K:::::K        i::::i         t:::::t     " +
                "        e:::::::::::::::::e");
        System.out.println("K:::::K K:::::K       i::::i         t:::::t     " +
                "        e::::::eeeeeeeeeee");
        System.out.println("KK::::::K  K:::::KKK    i::::i         t:::::t   " +
                " tttttt   e:::::::e");
        System.out.println("K:::::::K   K::::::K   i::::::i        " +
                "t::::::tttt:::::t   e::::::::e");
        System.out.println("K:::::::K    K:::::K   i::::::i        " +
                "tt::::::::::::::t    e::::::::eeeeeeee");
        System.out.println("K:::::::K    K:::::K   i::::::i          " +
                "tt:::::::::::tt     ee:::::::::::::e");
        System.out.println("KKKKKKKKK    KKKKKKK   iiiiiiii            " +
                "ttttttttttt         eeeeeeeeeeeeee");
    }
}
