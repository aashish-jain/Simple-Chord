package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
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
    String myHash;
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

    /* https://stackoverflow.com/questions/3105080/output-values-found-in-cursor-to-logcat-android */
    private static String cursorToString(Cursor cursor) {
        StringBuilder stringBuilder = new StringBuilder();
        int cursorCount = 1;
        if (cursor.moveToFirst()) {
            do {
                int columnsQty = cursor.getColumnCount();
                for (int idx = 0; idx < columnsQty; ++idx) {
                    stringBuilder.append(cursor.getString(idx));
                    if (idx < columnsQty - 1)
                        stringBuilder.append(",");
                }
                if (cursorCount < cursor.getCount())
                    stringBuilder.append("\n");
                cursorCount++;
            } while (cursor.moveToNext());
        }
        return stringBuilder.toString();
    }

    private static Cursor cursorFromString(String queryResult) {
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String key, value;
        String[] splitValues;
        for (String row : queryResult.split("\n")) {
            splitValues = row.split(",");
            try {
                key = splitValues[0];
                value = splitValues[1];
                matrixCursor.addRow(new String[]{key, value});
            } catch (Exception e){

            }
        }
        return matrixCursor;
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
        String keyHash = generateHash(key);
        // If has neighbours
        boolean toReturn = false;
        if (predecessor == null && successor == null)
            toReturn = true;
        else {
            // Hash of key lies in its range then return true else false
            //TODO: fix this condition for 1st node
            String predecessorHash = predecessor.getHashedId();

            /* Other Node - check if in hashspace */
            if (keyHash.compareTo(predecessorHash) > 0 && keyHash.compareTo(myHash) < 0)
                toReturn = true;
                /* First Node - check if in hashspace */
            else if (myHash.compareTo(predecessorHash) < 0 &&
                    (keyHash.compareTo(predecessorHash) > 0 || keyHash.compareTo(myHash) < 0))
                toReturn = true;
                /* Other node's hashspace */
            else
                toReturn = false;
            Log.d("HASHRANGE", "(" + predecessorHash + "," + myHash + "] +>" + keyHash +
                    " " + toReturn);
        }
        return toReturn;
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
        return deleteSingle(key);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        /* https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android */
        int deleted;
        if (selection.equals("@"))
            deleted = deleteAllLocal();
        else if (selection.equals("*"))
            deleted = deleteAll();
        else if (belongsToMe(selection))
            deleted = deleteSingle(selection);
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

    public void insertInDHT(ContentValues values) throws IOException {
        String key = values.getAsString("key"), value = values.getAsString("value");
        successor.oos.writeUTF(new Request(myID, key, value, RequestType.INSERT).toString());
        successor.oos.flush();
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(INSERT_TAG, values.toString());
        String key = values.getAsString("key");
        if (belongsToMe(key))
            insertLocal(values);
        else
            try {
                insertInDHT(values);
            } catch (IOException e) {
                e.printStackTrace();
            }
        return uri;
    }

    private void fetchNeighbours() {
        Request request = new Request(myID, RequestType.JOIN);
        Client client = null;
        try {
            client = new Client(serverId);
            client.oos.writeUTF(request.toString());
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

        myHash = generateHash(myID);

        Log.d(CREATE_TAG, "id is " + myID + " " + myHash);

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

    public Cursor queryAll() throws IOException {
        Cursor cursor = null;
        if (predecessor == null && successor == null)
            cursor = queryAllLocal();
        else {
            Request request = new Request(myID, RequestType.QUERY_ALL);
            successor.oos.writeUTF(request.toString());
            successor.oos.flush();
            cursor = cursorFromString(successor.ois.readUTF());
        }
        return cursor;
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

    public Cursor queryInDHT(String key) throws IOException {
        Request request = new Request(myID, key, null, RequestType.QUERY);
        successor.oos.writeUTF(request.toString());
        successor.oos.flush();
        String queryResult = successor.ois.readUTF();
        Cursor cursor = cursorFromString(queryResult);
        return cursor;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        /* https://developer.android.com/training/data-storage/sqlite */
        Log.d(QUERY_TAG, "Querying " + selection);
        Cursor cursor = null;
        /* Query for all local data*/
        if (selection.equals("*")) {
            try {
                cursor = queryAll();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if (selection.equals("@"))
            cursor = queryAllLocal();
        else if (belongsToMe(selection))
            cursor = querySingle(selection);
        else {
            try {
                cursor = queryInDHT(selection);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (cursor.getCount() == 0)
            Log.d(QUERY_TAG, selection + " not found :-(");
        else
            Log.d(QUERY_TAG, " Found = " + cursor.getCount() + "\n" + cursorToString(cursor));
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
                client.oos.writeUTF(new Request(senderId, RequestType.UPDATE_SUCCESSOR).toString());
                client.oos.flush();

                client = successorInRing.getValue();
                client.oos.writeUTF(new Request(senderId, RequestType.UPDATE_PREDECESSOR).toString());
                client.oos.flush();

                Log.d(TAG, "Flushed to " + senderId);
            }

            void insertHandler(Request request) throws IOException {
                Log.d(INSERT_TAG, request.toString());
                if (belongsToMe(request.getKey())) {
                    ContentValues values = new ContentValues();
                    values.put("key", request.getKey());
                    values.put("value", request.getValue());
                    insertLocal(values);
                    Log.d(TAG, "Will insert here - " + request.toString());
                } else {
                    successor.oos.writeUTF(request.toString());
                    successor.oos.flush();
                }
            }

            void queryHandler(Request request) throws IOException {
                Log.d(QUERY_TAG, request.toString());
                String key = request.getKey();
                String toReturn;
                if (belongsToMe(key)) {
                    Cursor cursor = querySingle(key);
                    toReturn = cursorToString(cursor);
                }
                else {
                    successor.oos.writeUTF(request.toString());
                    successor.oos.flush();
                    toReturn = successor.ois.readUTF();
                }
                Log.d(QUERY_TAG, toReturn);
                oos.writeUTF(toReturn);
                oos.flush();
            }

            void queryAllHandler(Request request) throws IOException {
                Log.d("QUERYALL", request.toString());
                String toReturn;
                if( request.getSenderId() != myID){
                    successor.oos.writeUTF(request.toString());
                    successor.oos.flush();
                    Log.d("QUERYALL", "Flushed. Waiting");
                    toReturn = successor.ois.readUTF() + "\n" + cursorToString(queryAllLocal());
                    Log.d("QUERYALL", "Got reply");
                }
                else {
                    toReturn = cursorToString(queryAllLocal());
                    Log.d("QUERYALL", "Starting Return\n" + toReturn);
                }
                Log.d("QUERYALL", "Returning");
                Log.d(QUERY_TAG, "\n" + toReturn);
                oos.writeUTF(toReturn);
                oos.flush();
            }

            void deleteHandler(Request request) throws IOException {
                Log.d(DELETE_TAG, request.toString());
                String key = request.getKey();
                if (belongsToMe(key)) {
                    deleteSingle(key);
                    Log.d(TAG, "Will delete here - " + request.toString());
                } else {
                    successor.oos.writeUTF(request.toString());
                    successor.oos.flush();
                }
            }

            void updateNeighbour(Request request) throws IOException {
                int newNeighbourId = request.getSenderId();

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
                    Log.d(TAG, "My new predecessor is " + newNeighbourId + " and hash" +
                            "range is (" + predecessor.getHashedId() + "," + myHash + "]");
                } else {
                    successor.close();
                    successor = new Client(newNeighbourId);
                    Log.d(TAG, "My new successor is " + newNeighbourId);
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
                    try {
                        switch (request.getRequestType()) {
                            case JOIN:
                                respondToJoinRequest(request);
                                break;
                            case QUIT:
                                Log.d(TAG, "Thread dying :-|");
                                return;
                            case QUERY:
                                queryHandler(request);
                                break;
                            case QUERY_ALL:
                                queryAllHandler(request);
                                break;
                            case INSERT:
                                insertHandler(request);
                                break;
                            case DELETE:
                                deleteHandler(request);
                                break;
                            case UPDATE_PREDECESSOR:
                            case UPDATE_SUCCESSOR:
                                updateNeighbour(request);
                                break;
                            default:
                                Log.d(TAG, "Unknown Operation. :-?");
                                return;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
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
            Request request = new Request(myID, RequestType.QUIT);
            oos.writeUTF(request.toString());
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
