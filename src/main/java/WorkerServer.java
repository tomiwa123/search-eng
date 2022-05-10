import static spark.Spark.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simple listener for worker creation
 *
 * @author zives
 *
 */
public class WorkerServer {
//    static Logger log = LogManager.getLogger(WorkerServer.class);
    static boolean alive;

    public WorkerServer(int myPort) {
        port(myPort);
        SearchServices.getSearchResults();
    }

    public static void shutdown() {
        alive = false;
    }

    /**
     * Simple launch for worker server.  Note that you may want to change / replace
     * most of this.
     *
     * @param args
     * @throws MalformedURLException
     */
    public static void main(String args[]) throws MalformedURLException {
        System.setProperty("aws.accessKeyId", System.getenv("AWS_ACCESS_KEY"));
        System.setProperty("aws.secretKey", System.getenv("AWS_SECRET_KEY"));

        if (args.length < 1) {
            System.out.println("Usage: WorkerServer [port number]");
            System.exit(1);
        }

        if (System.getenv("masterURL") == null) {
            System.out.println("Usage: flag -MasterURL");
            System.exit(1);
        }
        String masterURL = System.getenv("masterURL");

        int myPort = Integer.valueOf(args[0]);
//        String masterURL = args[1];
        alive = true;
        System.out.println("Worker node startup, on port " + myPort);

        // Worker: Compile search results
        WorkerServer workerServer = new WorkerServer(myPort);

        // TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce
        // here
        while (workerServer.alive){
            String urlString = String.format("http://%s/workerstatus?port=%d&&results=%s",
                    masterURL, myPort, "none");

//            log.info("Url is " + urlString);
            URL url = new URL(urlString);
            HttpURLConnection connection = null;
            try {
                System.out.println("Connecting to " + urlString);
                connection = (HttpURLConnection) url.openConnection();
                if (connection.getResponseCode() != 200){
//                    log.error("Failure to send a worker status update.");
                }
                Thread.sleep(10000);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
//        log.info("Worker at port" + myPort + " is shutting down");
        System.exit(0);
    }
}
