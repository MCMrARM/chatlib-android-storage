package io.mrarm.chatlib.android.storage;

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
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

    public void addMessage(String channel, MessageInfo message) {
        synchronized (this) {
            requireWrite();
            SQLiteStatement statement = createMessageStatements.get(channel);
            if (statement == null) {
                String tableName = MessagesContract.MessageEntry.getEscapedTableName(channel);
                database.execSQL(
                        "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                                MessagesContract.MessageEntry._ID + " INTEGER PRIMARY KEY," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_DATA + " TEXT NOT NULL," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_UUID + " BLOB NOT NULL," +
                                MessagesContract.MessageEntry.COLUMN_NAME_DATE + " INTEGER," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TEXT + " TEXT NOT NULL," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TYPE + " INTEGER," +
                                MessagesContract.MessageEntry.COLUMN_NAME_BATCH_UUID + " BLOB NOT NULL" +
                                ")");
                statement = database.compileStatement(
                        "INSERT INTO " + tableName + " (" +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_DATA + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_SENDER_UUID + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_DATE + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TEXT + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_TYPE + "," +
                                MessagesContract.MessageEntry.COLUMN_NAME_BATCH_UUID +
                                ") VALUES (?1,?2,?3,?4,?5,?6)");
                createMessageStatements.put(tableName, statement);
            }
            String senderData = message.getSender().getNickPrefixes().toString() + " " + message.getSender().getNick() + "!" + message.getSender().getUser() + "@" + message.getSender().getHost();
            statement.bindString(1, senderData);
            statement.bindBlob(2, uuidToBytes(message.getSender().getUserUUID()));
            statement.bindLong(3, message.getDate().getTime());
            statement.bindString(4, message.getMessage());
            statement.bindLong(5, message.getType().asInt());
            statement.bindBlob(6, uuidToBytes(message.getSender().getUserUUID()));
            statement.execute();
            statement.clearBindings();
        }
    }

    private static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer b = ByteBuffer.wrap(new byte[16]);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return b.array();
    }


}
