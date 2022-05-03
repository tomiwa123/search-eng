public class QueryResult {

    public String docID;
    public String url;
    public int tf;
    public float idf;
    public double pagerank;

    public QueryResult() {};

//    @Override
//    public int compareTo(Object o) {
//        if (!o.getClass().equals(this.getClass())) {
//            return 0;
//        }
//        if (this.pagerank < ((QueryResult) o).pagerank) {return 1;}
//        else if (qr_b.pagerank > qr_a.pagerank) {return -1;}
//        else return 0;
//    }
}
