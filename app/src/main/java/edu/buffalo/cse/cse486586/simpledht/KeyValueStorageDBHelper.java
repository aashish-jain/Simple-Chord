package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

//https://developer.android.com/training/data-storage/sqlite
class KeyValueStorageContract {
    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private KeyValueStorageContract() {}

    /* Inner class that defines the table contents */
    public static class KeyValueEntry implements BaseColumns {
        public static final String TABLE_NAME = "key_value";
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";
    }

    public static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + KeyValueEntry.TABLE_NAME + " (" +
                    KeyValueEntry.COLUMN_VALUE + " STRING," +
                    KeyValueEntry.COLUMN_KEY + " STRING PRIMARY KEY)";

    public static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + KeyValueEntry.TABLE_NAME;
}

//https://developer.android.com/training/data-storage/sqlite
class KeyValueStorageDBHelper extends SQLiteOpenHelper {
    // If you change the database schema, you must increment the database version.
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "KeyValue.db";

    public KeyValueStorageDBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(KeyValueStorageContract.SQL_CREATE_ENTRIES);
    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(KeyValueStorageContract.SQL_DELETE_ENTRIES);
        onCreate(db);
    }
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }
}