package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;

enum RequestType {
    JOIN, QUERY, INSERT, DELETE, QUIT, UPDATE_SUCCESSOR, UPDATE_PREDECESSOR;
}

public class Request {
    private static final String seperator = "<sep>", separator = " ";
    private String hashedQuery;
    private String query;
    private int senderId;
    String hashedSenderId;
    private RequestType requestType;
    static final String TAG = "REQUEST";

    Request(int senderId, String query, RequestType requestType) {
        this.senderId = senderId;
        this.hashedSenderId = SimpleDhtProvider.generateHash(Integer.toString(senderId));
        this.query = query;
        this.requestType = requestType;
        if (query != null)
            this.hashedQuery = SimpleDhtProvider.generateHash(query);
        else
            this.hashedQuery = null;
    }

    /* To parse from the string */
    Request(String string) throws IOException {
        String[] strings = string.split(this.seperator);
        if (strings.length == 5) {
            this.query = strings[0];
            this.requestType = RequestType.valueOf(strings[1]);
            this.hashedQuery = strings[2];
            this.senderId = Integer.parseInt(strings[3]);
            this.hashedSenderId = strings[4];
        } else {
            Log.d(TAG, string + " " + strings.length);
            throw new IOException("Unable to parse the String");
        }
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getQuery(){
        return query;
    }


    public int getSenderId() {
        return this.senderId;
    }

    public String getHashedSenderId() {
        return this.hashedSenderId;
    }

    public int getIntegerFromQuery(){
        return Integer.parseInt(query);
    }

    @Override
    public String toString() {
        return query + separator + requestType + separator + hashedQuery + separator +
                senderId + separator + hashedSenderId;
    }

    public String encode() {
        return query + seperator + requestType + seperator + hashedQuery + seperator +
                senderId + seperator + hashedSenderId;
    }
}
