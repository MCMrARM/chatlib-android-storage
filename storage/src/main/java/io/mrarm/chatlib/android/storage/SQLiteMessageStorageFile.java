package io.mrarm.chatlib.android.storage;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.mrarm.chatlib.android.storage.contract.MessagesContract;
import io.mrarm.chatlib.dto.MessageInfo;

public class SQLiteMessageStorageFile {

    private static final int AUTO_REMOVE_DELAY = 60 * 1000; // a minute
    private static final int CURRENT_DATABASE_VERSION = 1;

    private final SQLiteMessageStorageApi owner;
    private final long key;

    private int references = 0;
    private boolean removed = false;

    private final File file;
    private boolean readOnly;
    private SQLiteDatabase database;
    private final Map<String, SQLiteStatement> createMessageStatements = new HashMap<>();

    private Runnable removeRunnable = () -> close(true);

    public SQLiteMessageStorageFile(SQLiteMessageStorageApi owner, long key, File file,
                                    boolean readOnly) {
        this.owner = owner;
        this.key = key;
        this.file = file;
        this.readOnly = readOnly;
    }

    public boolean addReference() {
        synchronized (this) {
            if (removed)
                return false;
            if (references == 0)
                owner.getHandler().removeCallbacks(removeRunnable);
            references++;
        }
        return true;
    }

    public void removeReference() {
        synchronized (this) {
            references--;
            if (references == 0)
                owner.getHandler().postDelayed(removeRunnable, AUTO_REMOVE_DELAY);
        }
    }

    void close(boolean deleteFromOwner) {
        synchronized (this) {
            removed = true;
            if (deleteFromOwner) {
                synchronized (owner.files) {
                    if (owner.files.get(key) == SQLiteMessageStorageFile.this)
                        owner.files.remove(key);
                }
            }
            if (database != null)
                database.close();
        }
    }

    public void requireWrite() {
        synchronized (this) {
            if (database == null) {
                readOnly = false;
                openDatabase();
            } else if (readOnly) {
                database.close();
                readOnly = false;
                openDatabase();
            }
        }
    }

    private void openDatabase() {
        synchronized (this) {
            if (readOnly) {
                database = SQLiteDatabase.openDatabase(file.toString(), null, SQLiteDatabase.OPEN_READONLY);
            } else {
                database = SQLiteDatabase.openOrCreateDatabase(file, null);
                if (database.getVersion() == 0) {
                    createDatabaseTables();
                    database.setVersion(CURRENT_DATABASE_VERSION);
                }
            }
        }
    }

    private void createDatabaseTables() {
        //
    }

    public MessageQueryResult getMessages(String channel, int id, int offset, int limit) {
        synchronized (this) {
            if (database == null)
                openDatabase();

            String tableName = MessagesContract.MessageEntry.getEscapedTableName(channel);
            StringBuilder query = new StringBuilder();
            query.append("SELECT " +
                    MessagesContract.MessageEntry._ID + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_SENDER_DATA + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_SENDER_UUID + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_DATE + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_TEXT + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_TYPE + "," +
                    MessagesContract.MessageEntry.COLUMN_NAME_EXTRA_DATA +
                    " FROM ");
            query.append(tableName);
            if (id != -1) {
                query.append(" WHERE " + MessagesContract.MessageEntry._ID + "<");
                query.append(id);
            }
            query.append(" ORDER BY " + MessagesContract.MessageEntry._ID + " DESC");
            query.append(" LIMIT ");
            query.append(limit);
            if (offset != 0) {
                query.append(" OFFSET ");
                query.append(offset);
            }
            Cursor cursor = database.rawQuery(query.toString(), null);
            List<MessageInfo> ret = new ArrayList<>(cursor.getCount());
            int newAfterId = cursor.getInt(0);
            while (cursor.moveToNext()) {
                ret.add(MessageStorageHelper.deserializeMessage(
                        MessageStorageHelper.deserializeSenderInfo(cursor.getString(1), MessageStorageHelper.bytesToUUID(cursor.getBlob(2))),
                        new Date(cursor.getLong(3)),
                        cursor.getString(4),
                        cursor.getInt(5),
                        cursor.getString(6)
                ));
            }
            return new MessageQueryResult(ret, newAfterId);
        }
    }

    public void addMessage(String channel, MessageInfo message) {
        synchronized (this) {
            requireWrite();
            SQLiteStatement statement = createMessageStatements.get(channel);
            if (statement == null) {
                String tableName = MessagesContract.MessageEntry.getEscapedTableName(channel);
                database.execSQL(
                        "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                                MessagesContract.MessageEntry._ID + " INTEGER PRIMARY KEY," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_DATA + " TEXT," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_UUID + " BLOB," +
                                MessagesContract.MessageEntry.COLUMN_NAME_DATE + " INTEGER," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TEXT + " TEXT," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TYPE + " INTEGER," +
                                MessagesContract.MessageEntry.COLUMN_NAME_EXTRA_DATA + " TEXT" +
                                ")");
                statement = database.compileStatement(
                        "INSERT INTO " + tableName + " (" +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_DATA + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_UUID + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_DATE + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TEXT + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TYPE + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_EXTRA_DATA +
                                ") VALUES (?1,?2,?3,?4,?5,?6)");
                createMessageStatements.put(tableName, statement);
            }
            statement.bindString(1, MessageStorageHelper.serializeSenderInfo(message.getSender()));
            statement.bindBlob(2, MessageStorageHelper.uuidToBytes(message.getSender().getUserUUID()));
            statement.bindLong(3, message.getDate().getTime());
            if (message.getMessage() == null)
                statement.bindNull(4);
            else
                statement.bindString(4, message.getMessage());
            statement.bindLong(5, message.getType().asInt());
            statement.bindString(6, MessageStorageHelper.serializeExtraData(message));
            statement.execute();
            statement.clearBindings();
        }
    }

}
