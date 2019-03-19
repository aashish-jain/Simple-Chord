package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class Client {
    final String TAG = "CLIENT";
    ObjectInputStream ois;
    ObjectOutputStream oos;


    protected void establishConnection(int remoteProcessId){
        /* Establish the connection to server and store it in a Hashmap*/
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    remoteProcessId);
            this.ois = new ObjectInputStream(socket.getInputStream());
            this.oos = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            Log.e(TAG, "Unable to establish connection with the server");
        }
    }

    //TODO: sending joins
}
