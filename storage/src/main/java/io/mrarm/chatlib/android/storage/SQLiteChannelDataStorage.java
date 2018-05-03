package io.mrarm.chatlib.android.storage;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;

import java.util.concurrent.Future;

import io.mrarm.chatlib.android.storage.contract.ChannelDataContract;
import io.mrarm.chatlib.irc.ChannelDataStorage;

public class SQLiteChannelDataStorage implements ChannelDataStorage {

    private SQLiteMiscStorage storage;
    private SQLiteStatement getChannelStatement;
    private SQLiteStatement createChannelStatement;
    private SQLiteStatement updateTopicStatement;

    public SQLiteChannelDataStorage(SQLiteMiscStorage storage) {
        this.storage = storage;
    }

    @Override
    public Future<StoredData> getOrCreateChannelData(String channel) {
        return storage.getExecutor().queue(() -> {
            SQLiteDatabase db = storage.getDatabase();
            Cursor cursor = db.rawQuery(
                    "SELECT " + ChannelDataContract.ChannelEntry.COLUMN_NAME_TOPIC +
                            " FROM " + ChannelDataContract.ChannelEntry.TABLE_NAME +
                            " WHERE " + ChannelDataContract.ChannelEntry.COLUMN_NAME_CHANNEL
                            + "=?1",
                    new String[] { channel });
            if (!cursor.moveToFirst()) {
                if (createChannelStatement == null)
                    createChannelStatement = db.compileStatement("INSERT INTO " +
                            ChannelDataContract.ChannelEntry.TABLE_NAME + " (" +
                            ChannelDataContract.ChannelEntry.COLUMN_NAME_CHANNEL + ")" +
                            "VALUES (?1)");
                createChannelStatement.bindString(1, channel);
                createChannelStatement.executeInsert();
                createChannelStatement.clearBindings();
                return null;
            }
            int topicColumn = cursor.getColumnIndex(
                    ChannelDataContract.ChannelEntry.COLUMN_NAME_TOPIC);
            String topic = cursor.getString(topicColumn);
            return new StoredData(topic);
        }, null, null);
    }

    @Override
    public Future<Void> updateTopic(String channel, String topic) {
        return storage.getExecutor().queue(() -> {
            SQLiteDatabase db = storage.getDatabase();
            if (updateTopicStatement == null)
                updateTopicStatement = db.compileStatement(
                        "UPDATE " + ChannelDataContract.ChannelEntry.TABLE_NAME +
                        " SET " + ChannelDataContract.ChannelEntry.COLUMN_NAME_TOPIC + "=?2" +
                        " WHERE " + ChannelDataContract.ChannelEntry.COLUMN_NAME_CHANNEL + "=?1");
            updateTopicStatement.bindString(1, channel);
            updateTopicStatement.bindString(2, topic);
            updateTopicStatement.executeUpdateDelete();
            updateTopicStatement.clearBindings();
            return null;
        }, null, null);
    }

}
