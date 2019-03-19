package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;





public class SimpleDhtProvider extends ContentProvider {
    /* https://stackoverflow.com/questions/36652944/how-do-i-read-in-binary-data-files-in-java */
    static final String fileName = "database.db";
    static String[] columns = {"key", "value"};

    /* https://developer.android.com/training/data-storage/sqlite */
    KeyValueStorageDBHelper dbHelper;
    SQLiteDatabase dbWriter, dbReader;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        dbWriter.insertWithOnConflict(KeyValueStorageContract.KeyValueEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dbHelper = new KeyValueStorageDBHelper(getContext());
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        /* https://developer.android.com/training/data-storage/sqlite */

        /* Define a projection that specifies which columns from the database
         * you will actually use after this query.
         */
        projection = new String[] {
                KeyValueStorageContract.KeyValueEntry.COLUMN_KEY,
                KeyValueStorageContract.KeyValueEntry.COLUMN_VALUE
        };

        selectionArgs = new String[]{ selection };
        selection = KeyValueStorageContract.KeyValueEntry.COLUMN_KEY + " = ?";

        /* https://developer.android.com/training/data-storage/sqlite */
        Cursor cursor = dbReader.query(
                KeyValueStorageContract.KeyValueEntry.TABLE_NAME,   // The table to query
                projection,             // The array of columns to return (pass null to get all)
                selection,              // The columns for the WHERE clause
                selectionArgs,          // The values for the WHERE clause
                null,           // don't group the rows
                null,            // don't filter by row groups
                sortOrder               // The sort order
        );

        if(cursor.getCount() == 0)
            Log.e("QUERY", selectionArgs[0] + " not found :-(");

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
