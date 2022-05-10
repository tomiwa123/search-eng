
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;
import spark.Route;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.*;
import java.util.*;

import static spark.Spark.*;

public class MasterServer {
//    static Logger log = LogManager.getLogger(MasterServer.class);

    public static boolean alive = true;

    private static Map<String, Map> workerInfoMap = new HashMap<>();
    private static List<String> workerList = new ArrayList<>();
    private static Map<SearchRequest, String> currentJobs = new HashMap<>();

    private static String getWorkerList() {
        String output = "[";
        for (String worker: workerList) {
            output += worker + ",";
        }
        output = output.substring(0, output.length() - 1);
        output += "]";
        return output;
    }

    private static String getWorkerStatuses() {
        String output = "";
        int i = 0;
        for (String worker: workerList) {
            if (workerInfoMap.get(worker) == null) continue;
            Map<String, String> info = workerInfoMap.get(worker);
            output += i++ + ": port=" + info.get("port") + ", ";
            output += "status=" + info.get("status") + ", ";
            output += "job=" + info.get("job") + ", ";
            output += "keysRead=" + info.get("keysRead") + ", ";
            output += "keysWritten=" + info.get("keysWritten") + ", ";
            output += "results=" + info.get("results");
        }
        return output;
    }

    public static void registerStatusPage() {
        get("/status", (request, response) -> {
            response.type("text/html");

            String output = "<html><head><title>Master</title></head>\n" +
                    "<body>Hi, I am the master!";
            output += "<ul>" + getWorkerStatuses() + "</ul>";
//            output += "<form method=\"POST\" action=\"/submitjob\">\n" +
//                    "    Job Name: <input type=\"text\" name=\"jobname\"/><br/>\n" +
//                    "    Class Name: <input type=\"text\" name=\"classname\"/><br/>\n" +
//                    "    Input Directory: <input type=\"text\" name=\"input\"/><br/>\n" +
//                    "    Output Directory: <input type=\"text\" name=\"output\"/><br/>\n" +
//                    "    Map Threads: <input type=\"text\" name=\"map\"/><br/>\n" +
//                    "    Reduce Threads: <input type=\"text\" name=\"reduce\"/><br/>\n" +
//                    "<input type=\"submit\" value=\"Submit\"/>" +
//                    "</form>\r\n";

            output += "</body></html>";
            return output;
        });

    }

    public static void workerStatusPage() {
        get("/workerstatus", (request, response) -> {
            String results = request.queryParams("results");
            int port = Integer.valueOf(request.queryParams("port"));
            long timeNow = new Date().getTime();
            String ip = request.ip();

//            log.info("Received some worker status information");
            System.out.println("Received some worker status information");
            // TODO: Update the state that worker is alive
            String address = (ip + ":" + port);
            Map<String, String> info = new HashMap<>();
            if (!workerList.contains(address)) {
                workerList.add("http://" + address);
            }

            info.put("results", results);
            info.put("lastAlive", String.valueOf(timeNow));
            info.put("port", String.valueOf(port));
            workerInfoMap.put(address, info);
            return "";
        });
    }

    public static void homePage() {
        get("/", (req, res) -> {
            return "<html>" +
                    "<head>" +
                    "<link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\" integrity=\"sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3\" crossorigin=\"anonymous\">" +
                    "<style>\n" +
                    "  h1 {color:red;}\n" +
                    "  p {color:blue;}\n" +
                    "</style></head>\n" +

                    "<body>" +
                    "<div class=\"container\">" +
                    "<h2>GooseGooseStop</h2>" +
                    "<form method=\"POST\" action=\"submit\">" +
                    "<label for=\"search\">Search Engine:</label><br>" +
                    "<input type=\"text\" id=\"search\" name=\"search\"><br>" +
                    "<input type=\"submit\" value=\"Submit\">" +
                    "</form>" +
                    "</div>" +
                    "<script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js\" integrity=\"sha384-ka7Sk0Gln4gmtz2MlQnikT1wXgYsOg+OMhuP+IlRH9sENBO0LRn5q+8nbTov4+1p\" crossorigin=\"anonymous\"></script>" +
                    "</body></html>";
        });
    }

    private static String getContentFromURLConnection(URLConnection urlConnection) throws IOException {
        InputStream inputStream = urlConnection.getInputStream();
        BufferedReader buf = new BufferedReader(new InputStreamReader(inputStream));

        StringBuffer buffer = new StringBuffer();
        String line = buf.readLine();
        while (line != null) {
            buffer.append(line);
            buffer.append("\r\n");
            line = buf.readLine();
        }
        buf.close();
        return new String(buffer);
    }

    private static String getWorkerToHandleSearch(String ip, String searchQuery) {
        double index = Math.floor(Math.random() * workerList.size());
        return workerList.get(Integer.valueOf((int) index));
    }

    public static void handleSearch() {
        post("submit", new SubmitHandler());
        get("submit", new SubmitHandler());
    }

    public static class SubmitHandler implements Route {
        @Override
        public Object handle(Request request, Response response) throws Exception {
            String searchQuery = request.queryParams("search");
            String ip = request.ip();
            String address = getWorkerToHandleSearch(ip, searchQuery);

            searchQuery = searchQuery.replace(" ", "%20");

            // Make web call to the address
            String urlString = String.format(address + "/search?ip=%s&search=%s", ip, searchQuery);

//            log.info("Url is " + urlString);
            System.out.println("Url is " + urlString);

            URL url = new URL(urlString);
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                if (connection.getResponseCode() != 200){
//                    log.error("Failure to send a query to worker.");
                }
                String contentLines = getContentFromURLConnection(connection);
                return contentLines;
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.err.println("Issue with Worker result response");
            return "";
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.ip = ip;
//            searchRequest.searchQuery = searchQuery;
//            currentJobs.put(searchRequest, address);

        }
    }

    /**
     * The mainline for launching a MapReduce Master.  This should
     * handle at least the status and workerstatus routes, and optionally
     * initialize a worker as well.
     *
     * @param args
     */
    public static void main(String[] args) {

        System.setProperty("aws.accessKeyId", System.getenv("AWS_ACCESS_KEY"));
        System.setProperty("aws.secretKey", System.getenv("AWS_SECRET_KEY"));
        System.setProperty("aws.region", "us-east-1");

        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        String masterUrl = null;
        String workerUrl = null;

        try {
            masterUrl = "https://" + InetAddress.getLocalHost().getHostAddress();
            workerUrl = "https://" + InetAddress.getLocalHost().getHostAddress() + ":3030";

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        int myPort = Integer.valueOf(args[0]);
        port(myPort);

//        WorkerServer workerServer = new WorkerServer(3030);
//        workerList.add(workerUrl);

        System.out.println("Master node startup, on port " + myPort + " " + masterUrl);


        registerStatusPage();
        workerStatusPage();

        // Provide search
        homePage();

        // Receive search & return search results
        handleSearch();

        // Worker: Compile search results
        SearchServices.getSearchResults();

        while (alive) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        WorkerServer.shutdown();
        System.exit(0);

    }
}


