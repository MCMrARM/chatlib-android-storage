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
    private final List<MessageListener> globalListeners = new ArrayList<>();
    private final Map<String, List<MessageListener>> listeners = new HashMap<>();
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

    private SQLiteMessageStorageFile getCurrentFile() {
        return openFileFor(new Date(), false);
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
    public Future<Void> addMessage(String channel, MessageInfo messageInfo, ResponseCallback<Void> callback, ResponseErrorCallback errorCallback) {
        return executor.queue(() -> {
            openFileFor(messageInfo.getDate(), false).addMessage(channel, messageInfo);
            synchronized (listeners) {
                for (MessageListener listener : globalListeners)
                    listener.onMessage(channel, messageInfo);
                if (listeners.containsKey(channel)) {
                    for (MessageListener listener : listeners.get(channel))
                        listener.onMessage(channel, messageInfo);
                }
            }
            return null;
        }, callback, errorCallback);
    }

    @Override
    public Future<MessageList> getMessages(String channel, int count, MessageListAfterIdentifier after, ResponseCallback<MessageList> callback, ResponseErrorCallback errorCallback) {
        return null;
    }

    @Override
    public MessageListAfterIdentifier getMessageListAfterIdentifier(String channel, int count, MessageListAfterIdentifier after) {
        MyMessageListAfterIdentifier ret = new MyMessageListAfterIdentifier();
        if (after != null && after instanceof MyMessageListAfterIdentifier) {
            MyMessageListAfterIdentifier c = (MyMessageListAfterIdentifier) after;
            ret.file = c.file;
            ret.offset = c.offset;
        } else {
            ret.file = getCurrentFile();
        }
        ret.offset += count;
        return null;
    }

    @Override
    public Future<Void> subscribeChannelMessages(String channel, MessageListener messageListener, ResponseCallback<Void> callback, ResponseErrorCallback errorCallback) {
        synchronized (listeners) {
            if (channel != null) {
                if (!listeners.containsKey(channel))
                    listeners.put(channel, new ArrayList<>());
                listeners.get(channel).add(messageListener);
            } else
                globalListeners.add(messageListener);
        }
        return SimpleRequestExecutor.run(() -> null, callback, errorCallback);
    }

    @Override
    public Future<Void> unsubscribeChannelMessages(String channel, MessageListener messageListener, ResponseCallback<Void> callback, ResponseErrorCallback errorCallback) {
        synchronized (listeners) {
            if (channel != null) {
                if (listeners.containsKey(channel))
                    listeners.get(channel).remove(messageListener);
            } else
                globalListeners.remove(messageListener);
        }
        return SimpleRequestExecutor.run(() -> null, callback, errorCallback);
    }

    static class MyMessageListAfterIdentifier implements MessageListAfterIdentifier {

        public SQLiteMessageStorageFile file;
        public int offset;


    }

}
