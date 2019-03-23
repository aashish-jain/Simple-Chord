package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;

enum RequestType {
    JOIN, QUERY, INSERT, DELETE, QUIT, FETCH_PREDECESSOR, FETCH_SUCCESSOR, FETCH_NEW_NEIGHBOURS
}

public class Request {
    private static final String seperator = "<sep>", separator = " ";
    private String hashedQuery;
    private String query;
    private int senderId;
    String hashedSenderId;
    private RequestType requestType;


    Request(int senderId, String query, RequestType requestType) {
        this.senderId = senderId;
        this.hashedSenderId = SimpleDhtProvider.genHash(Integer.toString(senderId));
        this.query = query;
        this.requestType = requestType;
        if (query != null)
            this.hashedQuery = SimpleDhtProvider.genHash(query);
        else
            this.hashedQuery = null;
    }

    /* To parse from the string */
    Request(String string) throws IOException {
        String[] strings = string.split(this.seperator);
        if (strings.length == 4) {
            this.query = strings[0];
            this.requestType = RequestType.valueOf(strings[1]);
            this.hashedQuery = strings[2];
            this.senderId = Integer.parseInt(strings[3]);
            this.hashedSenderId = SimpleDhtProvider.genHash(this.query);
        } else
            throw new IOException("Unable to parse the String");
    }

    public RequestType getRequestType() {
        return requestType;
    }


    public int getSenderId() {
        return this.senderId;
    }

    public String getHashedSenderId() {
        return this.hashedSenderId;
    }

    @Override
    public String toString() {
        return query + separator + requestType + separator + hashedQuery + separator + senderId;
    }

    public String encode() {
        return query + seperator + requestType + seperator + hashedQuery + seperator + senderId;
    }
}
