package io.mrarm.chatlib.android.storage;

import android.os.Handler;

import io.mrarm.chatlib.ResponseCallback;
import io.mrarm.chatlib.ResponseErrorCallback;
import io.mrarm.chatlib.dto.MessageInfo;
import io.mrarm.chatlib.dto.MessageList;
import io.mrarm.chatlib.dto.MessageListAfterIdentifier;
import io.mrarm.chatlib.message.MessageListener;
import io.mrarm.chatlib.message.WritableMessageStorageApi;
import io.mrarm.chatlib.util.SimpleRequestExecutor;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;

public class SQLiteMessageStorageApi implements WritableMessageStorageApi {

    private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("'messages-'YYYY-MM-dd'.db'", Locale.getDefault());

    private final Handler handler = new Handler();
    private final SimpleRequestExecutor executor = new SimpleRequestExecutor();
    private final List<MessageListener> listeners = new ArrayList<>();
    final Map<Long, SQLiteMessageStorageFile> files = new HashMap<>();
    private final File directory;
    private SQLiteMessageStorageFile currentFile;

    public SQLiteMessageStorageApi(File directory) {
        this.directory = directory;
    }

    Handler getHandler() {
        return handler;
    }

    private long getDateIdentifier(Date date) {
        return date.getTime() / 1000 / 60 / 60 / 24;
    }

    private File getFilePathFor(Date date) {
        Date newDate = new Date(getDateIdentifier(date) * 1000 * 60 * 60 * 24);
        return new File(directory, fileNameFormat.format(date));
    }

    private SQLiteMessageStorageFile openFileFor(Date date, boolean readOnly) {
        long i = getDateIdentifier(date);
        synchronized (files) {
            SQLiteMessageStorageFile file = files.get(i);
            if (file != null && file.addReference())
                return file;
            file = new SQLiteMessageStorageFile(this, i, getFilePathFor(date), readOnly);
            files.put(i, file);
            return file;
        }
    }

    public void close() {
        synchronized (files) {
            for (SQLiteMessageStorageFile file : files.values()) {
                file.close(false);
            }
            files.clear();
        }
    }

    @Override
    public Future<Void> addMessage(String channel, MessageInfo messageInfo, ResponseCallback<Void> responseCallback, ResponseErrorCallback responseErrorCallback) {
        return executor.queue(() -> {
            openFileFor(messageInfo.getDate(), false).addMessage(channel, messageInfo);
            return null;
        }, responseCallback, responseErrorCallback);
    }

    @Override
    public Future<MessageList> getMessages(String s, int i, MessageListAfterIdentifier messageListAfterIdentifier, ResponseCallback<MessageList> responseCallback, ResponseErrorCallback responseErrorCallback) {
        return null;
    }

    @Override
    public MessageListAfterIdentifier getMessageListAfterIdentifier(String s, int i, MessageListAfterIdentifier messageListAfterIdentifier) {
        return null;
    }

    @Override
    public Future<Void> subscribeChannelMessages(String s, MessageListener messageListener, ResponseCallback<Void> responseCallback, ResponseErrorCallback responseErrorCallback) {
        synchronized (listeners) {
            listeners.add(messageListener);
        }
        return SimpleRequestExecutor.run(() -> null, responseCallback, responseErrorCallback);
    }

    @Override
    public Future<Void> unsubscribeChannelMessages(String s, MessageListener messageListener, ResponseCallback<Void> responseCallback, ResponseErrorCallback responseErrorCallback) {
        synchronized (listeners) {
            listeners.remove(messageListener);
        }
        return SimpleRequestExecutor.run(() -> null, responseCallback, responseErrorCallback);
    }

}
