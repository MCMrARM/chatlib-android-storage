package io.mrarm.chatlib.android.storage.contract;

public class ChannelDataContract {

    private ChannelDataContract() { }

    public static class ChannelEntry {

        public static final String TABLE_NAME = "channel_data";
        public static final String COLUMN_NAME_CHANNEL = "channel";
        public static final String COLUMN_NAME_TOPIC = "topic";

        public static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (" +
                COLUMN_NAME_CHANNEL + " TEXT PRIMARY KEY," +
                COLUMN_NAME_TOPIC + " TEXT)";

    }
}
