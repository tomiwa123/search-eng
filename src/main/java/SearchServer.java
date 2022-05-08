import static spark.Spark.*;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.math.BigDecimal;
import java.util.*;

public class SearchServer {

    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static DynamoDB docClient = new DynamoDB(client);
    private static int NUMBER_OF_DOCUMENTS = 1000000;

    public SearchServer() {

    }

    public static void main(String[] args) {
        port(3000);

        System.setProperty("aws.accessKeyId", "AWS_ACCESS_KEY");
        System.setProperty("aws.secretKey", "AWS_SECRET_KEY");

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
                    "<form method=\"POST\" action=\"search\">" +
                    "<label for=\"search\">Search Engine:</label><br>" +
                    "<input type=\"text\" id=\"search\" name=\"search\"><br>" +
                    "<input type=\"submit\" value=\"Submit\">" +
                    "</form>" +
                    "</div>" +
                    "<script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js\" integrity=\"sha384-ka7Sk0Gln4gmtz2MlQnikT1wXgYsOg+OMhuP+IlRH9sENBO0LRn5q+8nbTov4+1p\" crossorigin=\"anonymous\"></script>" +
                    "</body></html>";
        });

        post("search", (req, res) -> {

            String searchQuery = req.queryParams("search");

            List<String> searchQueryParseTerms = Arrays.asList(searchQuery.split(" "));
            if (searchQueryParseTerms == null) return printQueryResults(req.queryParams("search"), null);

            // Convert the search terms to lower case
            List<String> searchQueryTerms = new ArrayList<>();
            searchQueryParseTerms.forEach((String s) -> searchQueryTerms.add(s.toLowerCase(Locale.ROOT)));

            // Retrieve and Merge the Doc list for each query term
            List<QueryResult> queryResultCandidates = new LinkedList<>();
            for (String queryTerm: searchQueryTerms) {
                queryResultCandidates = mergeDocumentLists(queryResultCandidates, getDocumentList(queryTerm));
                if (queryResultCandidates == null) return printQueryResults(req.queryParams("search"), null);
            }

            // Retrieve the url link for each document
            List<QueryResult> queryResults = new LinkedList<>();
            for (QueryResult queryResult: queryResultCandidates) {
                // Add the document link
                QueryResult newQueryResult = getDocumentLink(queryResult);
                if (newQueryResult == null) {
                    System.err.println("Did not find docID " + queryResult.docID + " in database");
                } else {
                    queryResults.add(newQueryResult);
                }
            }

            // Retrieve pagerank score
            for (QueryResult queryResult: queryResults) {
                getPageRankScore(queryResult);
            }

            // Rank by score: tf * idf * pagerank
            queryResults.sort((qr_a, qr_b) -> {
                if (qr_a.pagerank * qr_a.tf * qr_a.idf < qr_b.pagerank * qr_b.tf * qr_b.idf) {return 1;}
                else if (qr_b.pagerank * qr_b.tf * qr_b.idf < qr_a.pagerank * qr_a.tf * qr_a.idf) {return -1;}
                else return 0;
            });

            // Filter for repeated domains and limit 20 results
            List<QueryResult> finalResults = new LinkedList<>();
            int resultNo = 0;
            Set<String> domains = new HashSet<>();

            for (QueryResult queryResult: queryResults) {
                if (inDomains(domains, new URLInfo(queryResult.url))) {
                    continue;
                }
                finalResults.add(queryResult);
                resultNo++;
                if (resultNo >= 20) {
                    break;
                }
            }

            // Return Query results
            return printQueryResults(req.queryParams("search"), finalResults);
        });
    }

    private static List<QueryResult> mergeDocumentLists(List<QueryResult> list_a, List<Item> list_b) {
        List<QueryResult> output = new LinkedList<>();
        int size = list_b.size();

        if (list_a.isEmpty()) {
            for (Item item: list_b) {
                QueryResult queryResult = new QueryResult();
                queryResult.docID = (String) item.get("doc");
                queryResult.tf = Integer.valueOf(item.get("tf").toString());
                queryResult.idf = NUMBER_OF_DOCUMENTS / size;
                output.add(queryResult);
            }
            if (output.isEmpty()) return null;
            return output;
        }

        for (QueryResult queryResult: list_a) {
            for (Item secondItem: list_b) {
                if (queryResult.docID.equals(secondItem.get("doc"))) {
                    queryResult.tf = queryResult.tf * Integer.valueOf(secondItem.get("tf").toString());
                    queryResult.idf = queryResult.idf * (NUMBER_OF_DOCUMENTS / size);
                    output.add(queryResult);
                    break;
                }
            }
        }
        if (output.isEmpty()) return null;
        return output;
    }

    private static List<Item> getDocumentList(String queryTerm) {
        Table termFrequencyTable = docClient.getTable("termFrequency");
//        Table termFrequencyTable = docClient.getTable("test-tf");
        Index termFrequencyIndex = termFrequencyTable.getIndex("word-tf-index");
        RangeKeyCondition rangeKeyCondition = new RangeKeyCondition("tf").gt(new BigDecimal(5));
        QuerySpec spec = new QuerySpec()
                .withHashKey("word", queryTerm)
                .withRangeKeyCondition(rangeKeyCondition)
                .withScanIndexForward(false);

        ItemCollection<QueryOutcome> items = termFrequencyIndex.query(spec);
        Iterator<Item> iterator = items.iterator();

        List<Item> output = new ArrayList<>();
        while (iterator.hasNext()) {
            output.add(iterator.next());
        }
        return output;
    }

    private static QueryResult getDocumentLink(QueryResult queryResult) {
        Table documentTable = docClient.getTable("document");

        Item item = documentTable.getItem("url_md5", queryResult.docID);
        if (item == null) return null;
        queryResult.url = (String) item.get("url");
        return queryResult;
    }

    private static void getPageRankScore(QueryResult queryResult) {
        Table pageRankTable = docClient.getTable("pageRank");

        Item item = pageRankTable.getItem("url_hash", queryResult.docID);
        if (item == null) {
            queryResult.pagerank = 1;
            return;
        }
        double pageRank = Float.valueOf(item.get("pagerank").toString());
        queryResult.pagerank = Math.log(Math.min(1 + pageRank, 10000));
//        System.out.println(queryResult.docID + " " + pageRank + " " + queryResult.pagerank);
    }

    private static String printQueryResults(String query, List<QueryResult> queryResults) {
        String output = "<html>" +
                "<head>" +
                "<link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\" integrity=\"sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3\" crossorigin=\"anonymous\">" +
                "<style>\n" +
                "  h1 {color:red;}\n" +
                "  p {color:blue;}\n" +
                "</style></head>\n" +

                "<body>" +
                "<div class=\"container\">" +
                "<h2>GooseGooseStop</h2>" +
                "<form method=\"POST\" action=\"search\">" +
                "<label for=\"search\">Search Engine:</label><br>" +
                "<input type=\"text\" id=\"search\" name=\"search\"><br>" +
                "<input type=\"submit\" value=\"Submit\">" +
                "</form>" +
                "</div><br /><br />";
        output += "<div class=\"container-md\"><h4> Query: " + query + "</h4>";
        output += "<ol>";
        if (!queryResults.isEmpty()) {
            for (QueryResult queryResult: queryResults) {
                output += "\n<li><p>" + "<a href=\"" + queryResult.url + "\" target=\"_blank\">" + queryResult.url + "</a> " +
                        "TF: " + queryResult.tf + " " + "IDF: " + queryResult.idf + " " +
                        "PageRank: " + queryResult.pagerank + "</p></li>";
            }
        }
        output += "</ol>";
        output += "</div>" +
                "<script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js\" integrity=\"sha384-ka7Sk0Gln4gmtz2MlQnikT1wXgYsOg+OMhuP+IlRH9sENBO0LRn5q+8nbTov4+1p\" crossorigin=\"anonymous\"></script>" +
                "</body></html>";

        return output;
    }

    private static boolean inDomains(Set<String> domains, URLInfo urlInfo) {
        for (String host: urlInfo.getHostName().split("\\.")) {
            for (String domain: domains) {
                if (domain.contains(host)) {
                    return true;
                }
            }
        }
        domains.add(urlInfo.getHostName());
        return false;
    }
}
