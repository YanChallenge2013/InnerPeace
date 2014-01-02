package rerank;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import utils.DatabaseConnecter;

//Algorithm to rerank pages using results from the same user
//Basically calculate the similarity between all the past queries and the current user
//Then
//1 Done. runSimWeightedKernel: using the similarity to weight every past operation
//2 Not Yet. 1NN: using the result from the 1st nearest neightbour
public class QueryNeighbour {
    private class UrlScorePair implements Comparable<UrlScorePair> {
        public long urlId;
        public double score;
        public UrlScorePair(long urlId, double score) {
            this.urlId = urlId;
            this.score = score;
        }
        
        @Override
        public int compareTo(UrlScorePair o) {
            if (this.score == o.score) {
                return 0;
            } else if (this.score < o.score) {
                return -1;
            } else {
                return 1;
            }
        }
    }
    
    private Connection conn = null;
    
    public HashMap< Long, List<Long> > rerank() throws SQLException {
        HashMap< Long, List<Long> > result = new HashMap< Long, List<Long> >();
        
        //The query to get all test serps, each with sessionId, serpId, userId and queryId
        String queryGetAllTest = "select sessionId as sessionId, serpId as serpId, userId as userId, queryId as queryId";
        
        conn = DatabaseConnecter.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryGetAllTest);
        while (rs.next()) {
            long sessionId = rs.getLong("sessionId");
            long serpId = rs.getLong("serpId");
            long userId = rs.getLong("userId");
            long queryId = rs.getLong("queryId");
            List rerankedList = runSimWeightedKernel(sessionId, serpId, userId, queryId);
            result.put(sessionId, rerankedList);
        }
        return result;
    }
    
    private List runSimWeightedKernel(long sessionId, long serpId, long userId, long queryId) throws SQLException {
        HashMap<Long, Double> querySimMap = new HashMap<Long, Double>();
        
        HashSet<Long> currentBag = getTermBag(queryId);
        
        //The query to get all distinct past queries of a given userId, each with queryId
        String queryGetPastQueries = "select queryId as queryId from table where userId =" + userId;
        
        Statement stmt1 = conn.createStatement();
        ResultSet rs1 = stmt1.executeQuery(queryGetPastQueries);
        while (rs1.next()) {
            long pastQueryId = rs1.getLong("queryId");
            HashSet<Long> pastBag = getTermBag(pastQueryId);
            double score = getCosSim(currentBag, pastBag);
            querySimMap.put(pastQueryId, score);
        }
        
        List<UrlScorePair> scoreList = new ArrayList<UrlScorePair>();
        
        //The query to get all urls of a given sessionId and serpId, each with urlId
        String queryGetUrls = "select urlId as urlId from table where sessionId =" + sessionId +
                " and serpId = " + serpId;
        Statement stmt2 = conn.createStatement();
        ResultSet rs2 = stmt2.executeQuery(queryGetUrls);
        while (rs2.next()) {
            long urlId = rs2.getLong("urlId");
            long expectedDwellTime = 0;
            
            //The query to get all click info of a given urlId and userId, each with queryId and dwell time
            String queryGetDwellTime = "select queryId as queryId, dwellTime as dwellTime from table where userId =" + 
                    userId + " and urlId = " + urlId;
            
            Statement stmt3 = conn.createStatement();
            ResultSet rs3 = stmt3.executeQuery(queryGetPastQueries);
            while (rs3.next()) {
                long thisQueryId = rs3.getLong("queryId");
                double weight = querySimMap.get(thisQueryId);
                long dwellTime = rs3.getLong("dwellTime");
                expectedDwellTime += dwellTime * weight;
            }
            scoreList.add(new UrlScorePair(urlId, expectedDwellTime));
        }
        Collections.sort(scoreList);
        
        List<Long> result = new ArrayList<Long>();
        for (int i = scoreList.size() - 1; i >= 0; i--) {
            result.add(scoreList.get(i).urlId);
        }
        return result;
    }
    
    private HashSet getTermBag(long queryId) throws SQLException {
        HashSet<Long> bag = new HashSet<Long>();
        
        //The query to get the terms of a given queryId, each with termId
        String queryGetTerms = "select termId as termId from table where queryId =" + queryId;
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryGetTerms);
        while (rs.next()) {
            long termId = rs.getLong("termId");
            bag.add(termId);
        }
        return bag;
    }
    
    private double getCosSim(HashSet<Long> hs1, HashSet<Long> hs2) {
        double score = 0;
        for (long termId : hs1) {
            if (hs2.contains(termId)) {
                score++;
            }
        }
        score /= hs1.size();
        score /= hs2.size();
        return score;
    }
    
    public static void main(String[] args) throws SQLException {
        QueryNeighbour qn = new QueryNeighbour();
        HashMap< Long, List<Long> > result = qn.rerank();
    }
}
