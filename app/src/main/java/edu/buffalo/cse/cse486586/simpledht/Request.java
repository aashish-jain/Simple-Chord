package edu.buffalo.cse.cse486586.simpledht;

import android.graphics.Path;

class Query{
    String query;
    private enum QueryType {
        ALL, ALL_LOCAL, ONE
    }
    QueryType queryType;

    Query(String query){
        this.query = query;
        if(query.equals("*"))
            queryType = QueryType.ALL;
        else if(query.equals("@"))
            queryType = QueryType.ALL_LOCAL;
        else
            queryType = QueryType.ONE;
    }

    public boolean isAll(){
        return queryType.equals(QueryType.ALL);
    }

    public boolean isAllLocal(){
        return queryType.equals(QueryType.ALL_LOCAL);
    }
    public boolean isOne(){
        return queryType.equals(QueryType.ONE);
    }
}


enum Operation {
    QUERY, DELETE
}

public class Request {
    String query;

    private enum RequestType {
        JOIN, QUERY
    }
    RequestType requestType;

    Operation operation;

    Request(String query, boolean join, Operation operation) {
        this.query = query;
        if (!join) {
            this.requestType = RequestType.QUERY;
        } else {
            this.requestType = RequestType.JOIN;
        }
        this.operation = operation;
    }

    public boolean isJoin(){
        return requestType.equals(RequestType.JOIN);
    }

    public boolean isQuery(){
        return requestType.equals((RequestType.QUERY));
    }

}
