package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;

public class SimpleDhtProvider extends ContentProvider {
    /* https://developer.android.com/training/data-storage/sqlite */
    KeyValueStorageDBHelper dbHelper;
    SQLiteDatabase dbWriter, dbReader;
    static final int selfProcessIdLen = 4;
    int myID;
    String myHashedID;
    Client successor, predecessor;
    static final int serverId = 5554;
    TreeMap<String, Client> chordRingMap;
    static final String DELETE_TAG = "DELETE_TAG", CREATE_TAG = "CREATE_TAG",
            INSERT_TAG = "INSERT_TAG", QUERY_TAG = "QUERY_TAG", FETCH_TAG = "FETCH";

    static final String[] projection = new String[]{
            KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
            KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };


    public static String generateHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash)
            formatter.format("%02x", b);
        return formatter.toString();
    }

    public static String generateHash(int number) {
        String input = Integer.toString(number);
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash)
            formatter.format("%02x", b);
        return formatter.toString();
    }

    public static void enableStrictMode() {
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
    }

    private int getProcessId() {
        /* https://stackoverflow.com/questions/10115533/how-to-getsystemservice-within-a-contentprovider-in-android */
        String telephoneNumber =
                ((TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE)).getLine1Number();
        int length = telephoneNumber.length();
        telephoneNumber = telephoneNumber.substring(length - selfProcessIdLen);
        int id = Integer.parseInt(telephoneNumber);
        return id;
    }

    private boolean belongsToMe(String key) {
        // If has neighbours
        if (predecessor != null && successor != null) {
            // Hash of key lies in its range then return true else false
            //TODO: fix this condition for 1st node
            if (key.compareTo(predecessor.getHashedId()) > 0 && key.compareTo(myHashedID) < 0)
                return true;
            else
                return false;
        }
        //else return true
        else
            return true;
    }

    public int deleteSingle(String key) {
        //https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + "='" + key + "'",
                null);
    }

    public int deleteAllLocal() {
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, null);
    }

    public int deleteAll() {
//        Log.d(DELETE_TAG, "Not yet implemented");
//        return 0;
        return deleteAllLocal();
    }

    public int deleteFromDHT(String key) {
//        Log.d(DELETE_TAG, "Not yet implemented");
//        return 0;
        return deleteSingle(key);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        /* https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android */
        int deleted;
        if (belongsToMe(selection))
            deleted = deleteSingle(selection);
        else if (selection.equals("@"))
            deleted = deleteAllLocal();
        else if (selection.equals("*"))
            deleted = deleteAll();
        else
            deleted = deleteFromDHT(selection);
        Log.d(DELETE_TAG, "Removed " + deleted + " messages");
        return deleted;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    public long insertLocal(ContentValues values) {
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    public long insertInDHT(ContentValues values) {
//        Log.d(INSERT_TAG, "Not yet implemented");
//        return 0;
        return insertLocal(values);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT_TAG, values.toString());
        if (belongsToMe(values.getAsString("key")))
            insertLocal(values);
        else
            insertInDHT(values);
        return uri;
    }

    private void fetchNeighbours() {
        Request request = new Request(myID, null, RequestType.JOIN);
        Client client = null;
        try {
            client = new Client(serverId);
            client.oos.writeUTF(request.encode());
            client.oos.flush();
            int predId = client.ois.readInt();
            int succId = client.ois.readInt();
            Log.d(FETCH_TAG, "Updated successor and predecessor. " + predId + " " + succId);
            predecessor = new Client(predId);
            successor = new Client(succId);
            Log.d(FETCH_TAG, "Set");
            client.close();
        } catch (IOException e) {
            Log.d("CLIENT", "avd 0 may not be up");
        }
    }

    @Override
    public boolean onCreate() {
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();
        successor = null;
        predecessor = null;

        myHashedID = generateHash(myID);

        Log.d(CREATE_TAG, "id is " + myID + " " + myHashedID);

        new Server().start();

        /* Small delay to ensure that the server of the node starts
         * This is to ensure that the AVD0's server is up and
         * it can connect to it
         * */
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* Because all calls are blocking calls running it on the main thread*/
        enableStrictMode();

        /* If not server ask the server for new Nodes*/
        if (myID != serverId)
            fetchNeighbours();
            /* Else just send them */
        else {
            chordRingMap = new TreeMap<String, Client>();
            try {
                chordRingMap.put(generateHash(myID), new Client(myID));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public Cursor queryAllLocal() {
        //Query everything
        return dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                null, null, null, null,
                null, null);
    }

    public Cursor queryAll() {
//        Log.d(QUERY_TAG, "Not yet implemented");
//        return null;
        return queryAllLocal();
    }

    public Cursor querySingle(String key) {
        String[] selectionArgs = new String[]{key};
        String selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

        /* https://developer.android.com/training/data-storage/sqlite */
        return dbReader.query(
                KeyValueStorageContract.KeyValueEntry.TABLE_NAME,   // The table to query
                this.projection,        // The array of columns to return (pass null to get all)
                selection,              // The columns for the WHERE clause
                selectionArgs,          // The values for the WHERE clause
                null,           // don't group the rows
                null,            // don't filter by row groups
                null               // The sort order
        );
    }

    public Cursor queryInDHT(String key) {
        return querySingle(key);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        /* https://developer.android.com/training/data-storage/sqlite */
        Log.d(QUERY_TAG, "Querying " + selection);
        Cursor cursor = null;
        /* Query for all local data*/
        if (selection.equals("*"))
            cursor = queryAll();
        else if (selection.equals("@"))
            cursor = queryAllLocal();
        else if (belongsToMe(selection))
            cursor = querySingle(selection);
        else
            cursor = queryInDHT(selection);
        if (cursor.getCount() == 0)
            Log.d(QUERY_TAG, selection + " not found :-(");
        else
            Log.d(QUERY_TAG, " Found = " + cursor.getCount() + " " + DatabaseUtils.dumpCursorToString(cursor));
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private class Server extends Thread {
        static final int SERVER_PORT = 10000;
        static final String TAG = "SERVER_TASK";

        /* https://stackoverflow.com/questions/10131377/socket-programming-multiple-client-to-one-server*/
        private class ServerThread extends Thread {
            ObjectOutputStream oos;
            ObjectInputStream ois;

            static final String TAG = "SERVER_THREAD";

            public ServerThread(Socket socket) {
                try {
                    this.ois = new ObjectInputStream(socket.getInputStream());
                    this.oos = new ObjectOutputStream(socket.getOutputStream());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            public synchronized void respondToJoinRequest(Request request) throws IOException {
                int senderId = request.getSenderId();
                String senderHash = request.getHashedSenderId();

                Log.d(TAG, "Recieved Join request from " + senderId);
                /* Safe-check to ensure that only 5554 handles the join request*/
                if (myID != serverId)
                    return;

                Map.Entry<String, Client> predecessorInRing, successorInRing;
                chordRingMap.put(senderHash, new Client(senderId));
                Log.d(TAG, "Chord ring size is now " + chordRingMap.size());

                predecessorInRing = chordRingMap.lowerEntry(senderHash);
                successorInRing = chordRingMap.higherEntry(senderHash);

                //Enforce the neighbours
                successorInRing = (successorInRing != null) ?
                        successorInRing : chordRingMap.firstEntry();
                predecessorInRing = (predecessorInRing != null) ?
                        predecessorInRing : chordRingMap.lastEntry();

                int predecessorId = predecessorInRing.getValue().getConnectedId();
                int successorId = successorInRing.getValue().getConnectedId();
                Log.d(TAG, senderId + " Got predId = " + predecessorId + " succId = " + successorId);

                /* Sends back the successor and predecessor to the requesting node*/
                oos.writeInt(predecessorId);
                oos.writeInt(successorId);
                oos.flush();

                /* Ask the neighbours to consider the new node as successor or predecessor */
                Client client = predecessorInRing.getValue();
                client.oos.writeUTF(new Request(senderId, null, RequestType.UPDATE_SUCCESSOR).encode());
                client.oos.flush();

                client = successorInRing.getValue();
                client.oos.writeUTF(new Request(senderId, null, RequestType.UPDATE_PREDECESSOR).encode());
                client.oos.flush();

                Log.d(TAG, "Flushed to " + senderId);
            }

            void updateNeighbour(Request request) {
                int newNeighbourId = request.getSenderId();

                try {
                    /* Run when no new node exists only for avd 0*/
                    if (predecessor == null && successor == null) {
                        predecessor = new Client(newNeighbourId);
                        successor = new Client(newNeighbourId);
                        return;
                    }

                    /* Ask the already connected threads to quit*/
                    if (request.getRequestType() == RequestType.UPDATE_PREDECESSOR) {
                        predecessor.close();
                        predecessor = new Client(newNeighbourId);
                    } else {
                        successor.close();
                        successor = new Client(newNeighbourId);
                    }
                    Log.d(TAG, "Latest " + request.getRequestType() + " is " + newNeighbourId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void run() {
                //Read from the socket
                while (true) {
                    Request request = null;
                    try {
                        request = new Request(ois.readUTF());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    switch (request.getRequestType()) {
                        case JOIN:
                            try {
                                respondToJoinRequest(request);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            break;
                        case QUIT:
                            Log.d(TAG, "Dying");
                            return;
                        case QUERY:
                            break;
                        case INSERT:
                            break;
                        case DELETE:
                            break;
                        case UPDATE_PREDECESSOR:
                        case UPDATE_SUCCESSOR:
                            updateNeighbour(request);
                            break;
                        default:
                            Log.d(TAG, "Unknown Operation. :-/");
                            return;

                    }
                }
            }
        }

        public void run() {
            /* Open a socket at SERVER_PORT */
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(SERVER_PORT);
                Log.d(TAG, "Server started Listening");
            } catch (IOException e) {
                e.printStackTrace();
            }

            /* Accept a client connection and spawn a thread to respond */
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    Log.d(TAG, "Incoming connection....");
                    new ServerThread(socket).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Client {
        private static final String TAG = "CLIENT";
        private int connectedId;
        private String hashedConnectedId;
        private Socket socket;
        private ObjectInputStream ois;
        private ObjectOutputStream oos;
        boolean isConnected;

        Client(int remoteProcessId) throws IOException {
            /* Establish the connection to server and store it in a Hashmap*/
            connectedId = remoteProcessId;
            hashedConnectedId = generateHash(remoteProcessId);
            isConnected = false;
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    remoteProcessId * 2);
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            isConnected = true;
        }

        void close() throws IOException {
            Request request = new Request(myID, null, RequestType.QUIT);
            oos.writeUTF(request.encode());
            oos.flush();
            oos.close();
            ois.close();
            socket.close();
        }

        public String getHashedId() {
            return hashedConnectedId;
        }

        public int getConnectedId() {
            return connectedId;
        }
    }
}
