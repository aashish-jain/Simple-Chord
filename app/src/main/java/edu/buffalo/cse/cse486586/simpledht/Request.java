package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;

enum RequestType {
    JOIN, QUERY, QUERY_ALL ,INSERT, DELETE, QUIT, UPDATE_SUCCESSOR, UPDATE_PREDECESSOR;
}

public class Request {
    static final String TAG = "REQUEST";
    private static final String seperator = ",";

    private int senderId;
    private String hashedSenderId;
    private RequestType requestType;
    private String key, value;

    Request(int senderId, RequestType requestType) {
        this.senderId = senderId;
        this.hashedSenderId = SimpleDhtProvider.generateHash(Integer.toString(senderId));
        this.requestType = requestType;
        this.key = null;
        this.value = null;
    }

    Request(int senderId, String key, String value, RequestType requestType){
        this.senderId = senderId;
        this.hashedSenderId = SimpleDhtProvider.generateHash(Integer.toString(senderId));
        this.requestType = requestType;
        this.key = key;
        this.value = value;
    }

    /* To parse from the string */
    Request(String string) throws IOException {
        String[] strings = string.split(this.seperator);
        if (strings.length == 5) {
            this.requestType = RequestType.valueOf(strings[0]);
            this.senderId = Integer.parseInt(strings[1]);
            this.hashedSenderId = strings[2];
            this.key = strings[3];
            this.value = strings[4];
        } else {
            Log.d(TAG, string + " " + strings.length);
            throw new IOException("Unable to parse the String");
        }
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getSenderId() {
        return this.senderId;
    }

    public String getHashedSenderId() {
        return this.hashedSenderId;
    }

    @Override
    public String toString() {
        return requestType + seperator + senderId + seperator + hashedSenderId + seperator +
                key + seperator + value;
    }
}
