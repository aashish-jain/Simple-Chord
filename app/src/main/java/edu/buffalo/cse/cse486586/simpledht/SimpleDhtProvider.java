package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
//import android.database.DatabaseUtils;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
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

    private int getProcessId(){
        /* https://stackoverflow.com/questions/10115533/how-to-getsystemservice-within-a-contentprovider-in-android */
        String telephoneNumber =
                ((TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE)).getLine1Number();
        int length = telephoneNumber.length();
        telephoneNumber = telephoneNumber.substring(length - selfProcessIdLen);
        int id = Integer.parseInt(telephoneNumber);
        return id;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        final String TAG = "DELETE_TAG";
        /* https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android */
        int deleted = 0;

        if( selection.equals("@") || selection.equals("*"))
            deleted = dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                    null, null);
        else {
            deleted = dbWriter.delete(KeyValueStorageContract.KeyValueEntry.TABLE_NAME,
                    KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + "='" + selection + "'",
                        null);
//                    new String[]{selection});
        }
        Log.d(TAG, "Removed "+ deleted);
        return deleted;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //TODO: add lookup and fix *
        final String TAG = "INSERT_TAG";
        Log.d(TAG, values.toString());

        dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
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

        if(myID != 5554){
            try {
                /* Safety mechanism to ensure that the join request is sent only after 5554 is active*/
                Thread.sleep(1000);
                //TODO: Connection request to 5554*2
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        /* https://developer.android.com/training/data-storage/sqlite */

        /* Define a projection that specifies which columns from the database
         * you will actually use after this query.
         */

        final String TAG = "QUERY_TAG";
        Log.d(TAG, "Querying " + selection);
        Cursor cursor = null;
        /* Query for all local data*/
        if(selection.equals("@") || selection.equals("*")){
            //Query everything
            cursor = dbReader.query(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, this.projection,
                    null, null, null, null,
                    null, null);
        }
        else{
            //TODO: add lookup and fix *
            selectionArgs = new String[]{ selection };
            selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

            /* https://developer.android.com/training/data-storage/sqlite */
            cursor = dbReader.query(
                    KeyValueStorageContract.KeyValueEntry.TABLE_NAME,   // The table to query
                    this.projection,             // The array of columns to return (pass null to get all)
                    selection,              // The columns for the WHERE clause
                    selectionArgs,          // The values for the WHERE clause
                    null,           // don't group the rows
                    null,            // don't filter by row groups
                    sortOrder               // The sort order
            );
        }
        if (cursor.getCount() == 0)
            Log.d(TAG, selectionArgs[0] + " not found :-(");
        else
            Log.d(TAG, " Found = " + cursor.getCount() +" "+ DatabaseUtils.dumpCursorToString(cursor));
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

}
