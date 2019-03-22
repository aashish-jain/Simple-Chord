package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

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

public class SimpleDhtProvider extends ContentProvider {
    /* https://developer.android.com/training/data-storage/sqlite */
    KeyValueStorageDBHelper dbHelper;
    SQLiteDatabase dbWriter, dbReader;
    static final int selfProcessIdLen = 4;
    int myID;
    String myHashedID;
    ChordNeighbour successor, predecessor;
    static final String DELETE_TAG = "DELETE_TAG", CREATE_TAG = "CREATE_TAG", INSERT_TAG = "INSERT_TAG", QUERY_TAG = "QUERY_TAG";

    static String[] projection = new String[] {
        KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
                KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
    };

    public static String genHash(String input) {
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

    private int getProcessId(){
        /* https://stackoverflow.com/questions/10115533/how-to-getsystemservice-within-a-contentprovider-in-android */
        String telephoneNumber =
                ((TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE)).getLine1Number();
        int length = telephoneNumber.length();
        telephoneNumber = telephoneNumber.substring(length - selfProcessIdLen);
        int id = Integer.parseInt(telephoneNumber);
        return id;
    }

    private boolean belongsToMe(String key){
        //TODO: finish the belongs to me logic
        key = genHash(key);
        // If has neighbours
        if(predecessor != null && successor!=null) {
            // Hash of key lies in its range then return true else false
            if (key.compareTo(predecessor.getHashedID()) > 0 && key.compareTo(myHashedID) < 0)
                return true;
            else
                return false;
        }
        //else return true
        else
            return true;
    }

    public int deleteSingle(String key){
        //https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + "='" + key + "'",
                null);
    }

    public int deleteAllLocal(){
        return dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, null);
    }

    public int deleteAll(){
        Log.d(DELETE_TAG, "Not yet implemented");
        return 0;
    }

    public int deleteFromDHT(String key){

        Log.d(DELETE_TAG, "Not yet implemented");
        return 0;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        /* https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android */
        int deleted;
        if(belongsToMe(selection))
            deleted = deleteSingle(selection);
        else if( selection.equals("@"))
            deleted = deleteAllLocal();
        else if(selection.equals("*"))
            deleted = deleteAll();
        else
            deleted = deleteFromDHT(selection);
        Log.d(DELETE_TAG, "Removed "+ deleted + " messages");
        return deleted;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }


    public long insertLocal(ContentValues values){
        return dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                null, values, SQLiteDatabase.CONFLICT_REPLACE);
    }

    public long insertInDHT(ContentValues values){
        Log.d(INSERT_TAG, "Not yet implemented");
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //TODO: add lookup
        Log.d(INSERT_TAG, values.toString());
        if(belongsToMe(values.getAsString("key")))
            insertLocal(values);
        else
            insertInDHT(values);
        return uri;
    }

    @Override
    public boolean onCreate() {
        final String TAG = "CREATE_TAG";
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        myID = getProcessId();
        successor = null;
        predecessor = null;

        myHashedID = genHash(Integer.toString(myID));

        Log.d(TAG, "id is " + myID + " " + myHashedID);

        //TODO: start the server thread
        new Server().start();

        enableStrictMode();
        int joinServerPort = 11108;
        if(myID != 5554){
            Request request = new Request(myID,null, RequestType.JOIN);
            Log.d(TAG, "Attempting to join");
            Client client = new Client(joinServerPort);
            client.sendJoinRequest(request);
            Log.d(TAG, "Joined");
            client.sendRequest(new Request(0, null, RequestType.QUIT));
            //TODO: Connection request to 5554*2

        }

        return true;
    }

    public Cursor queryAllLocal(){
        //Query everything
        return dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                null, null, null, null,
                null, null);
    }

    public Cursor queryAll(){
        Log.d(QUERY_TAG, "Not yet implemented");
        return null;
    }

    public Cursor querySingle(String key){
        //TODO: add lookup and fix *
        String[] selectionArgs = new String[]{ key };
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



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        /* https://developer.android.com/training/data-storage/sqlite */
        Log.d(QUERY_TAG, "Querying " + selection);
        Cursor cursor = null;
        /* Query for all local data*/
        if(selection.equals("*"))
            cursor = queryAll();
        else if(selection.equals("@"))
            cursor = queryAllLocal();
        else if(belongsToMe(selection)){
            querySingle(selection);
        }
        else{

        }
        if (cursor.getCount() == 0)
            Log.d(QUERY_TAG, selectionArgs[0] + " not found :-(");
        else
            Log.d(QUERY_TAG, " Found = " + cursor.getCount() +" "+ DatabaseUtils.dumpCursorToString(cursor));
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


            public  ServerThread(Socket socket) {
                try {
                    this.ois = new ObjectInputStream(socket.getInputStream());
                    this.oos = new ObjectOutputStream(socket.getOutputStream());
                    Log.d(TAG, "Server started");
                } catch (Exception e) {
                    Log.e(TAG, "Failure");
                }
            }

            @Override
            public void run() {
                //Read from the socket
                try {
                    //TODO: Make server respond to queries
                    while (true) {
                        Request request = new Request(ois.readUTF());
                        if(request.isJoin()){
                            Log.d(TAG, "Recieved Join request from " + request.getSenderId());
                        }
                        if(request.isQuit()) {
                            Log.d(TAG, "Dying");
                            break;
                        }
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Failure");
                }
            }
        }

        public void run() {
            /* Open a socket at SERVER_PORT */
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(SERVER_PORT);
            } catch (IOException e) {
                e.printStackTrace();
            }

            /* Accept a client connection and spawn a thread to respond */
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    new ServerThread(socket).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
