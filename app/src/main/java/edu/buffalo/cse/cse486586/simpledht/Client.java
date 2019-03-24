package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class Client {
    private final String TAG = "CLIENT";
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    boolean isConnected;

    Client(int remoteProcessId) {
        /* Establish the connection to server and store it in a Hashmap*/
        Socket socket = null;
        isConnected = false;
        try {
            Log.d(TAG, "Attempting connection");
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    remoteProcessId);
            Log.d(TAG, "Connected to server");
            this.oos = new ObjectOutputStream(socket.getOutputStream());
            this.ois = new ObjectInputStream(socket.getInputStream());
            isConnected = true;
        } catch (IOException e) {
            Log.e(TAG, "Unable to establish connection with the server");
        }
    }

    public void sendJoinRequest(Request request) {
        try {
            Log.d(TAG, request.toString());
            oos.writeUTF(request.encode());
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendRequest(Request request) {
        try {
            oos.writeUTF(request.encode());
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int readInt() {
        int val = 0;
        try {
            val = ois.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return val;
    }
}
