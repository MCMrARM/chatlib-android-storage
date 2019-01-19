package io.mrarm.chatlib.android.storage;

import android.os.Handler;

import io.mrarm.chatlib.ResponseCallback;
import io.mrarm.chatlib.ResponseErrorCallback;
import io.mrarm.chatlib.dto.MessageFilterOptions;
import io.mrarm.chatlib.dto.MessageInfo;
import io.mrarm.chatlib.dto.MessageList;
import io.mrarm.chatlib.dto.MessageListAfterIdentifier;
import io.mrarm.chatlib.message.MessageListener;
import io.mrarm.chatlib.message.WritableMessageStorageApi;
import io.mrarm.chatlib.util.SimpleRequestExecutor;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;

public class SQLiteMessageStorageApi implements WritableMessageStorageApi {

    private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("'messages-'yyyy-MM-dd'.db'", Locale.getDefault());

    private final Handler handler = new Handler();
    private final SimpleRequestExecutor executor = new SimpleRequestExecutor();
    private final List<MessageListener> globalListeners = new ArrayList<>();
    private final Map<String, List<MessageListener>> listeners = new HashMap<>();
    final Map<Long, SQLiteMessageStorageFile> files = new HashMap<>();
    private final SortedSet<Long> availableFiles = new TreeSet<>(Collections.reverseOrder());
    private final File directory;
    private SQLiteMessageStorageFile currentFile;

    public SQLiteMessageStorageApi(File directory) {
        this.directory = directory;
        open();
    }

    Handler getHandler() {
        return handler;
    }

    private long getDateIdentifier(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.DATE) + c.get(Calendar.MONTH) * 32 + c.get(Calendar.YEAR) * 32 * 12;
    }

    private File getFilePathFor(long dateId) {
        Calendar c = Calendar.getInstance();
        c.set((int) (dateId / 32 / 12), (int) ((dateId / 32) % 12), (int) (dateId % 32));
        return new File(directory, fileNameFormat.format(c.getTime()));
    }

    private long getDateIdFromFileName(String fileName) throws ParseException {
        return getDateIdentifier(fileNameFormat.parse(fileName));
    }

    private SQLiteMessageStorageFile openFileFor(long dateId, boolean readOnly) {
        synchronized (files) {
            SQLiteMessageStorageFile file = files.get(dateId);
            if (file != null && file.addReference())
                return file;
            if (!readOnly)
                availableFiles.add(dateId);
            file = new SQLiteMessageStorageFile(this, dateId, getFilePathFor(dateId), readOnly);
            file.addReference();
            files.put(dateId, file);
            return file;
        }
    }

    private SQLiteMessageStorageFile openFileFor(Date date, boolean readOnly) {
        return openFileFor(getDateIdentifier(date), readOnly);
    }

    public void open() {
        synchronized (files) {
            directory.mkdirs();
            availableFiles.clear();
            for (File child : directory.listFiles()) {
                if (child.isFile()) {
                    try {
                        availableFiles.add(getDateIdFromFileName(child.getName()));
                    } catch (ParseException ignored) {
                    }
                }
            }
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
    public Future<Void> addMessage(String channel, MessageInfo messageInfo, ResponseCallback<Void> callback, ResponseErrorCallback errorCallback) {
        return executor.queue(() -> {
            SQLiteMessageStorageFile file = openFileFor(new Date(), false);
            file.addMessage(channel, messageInfo);
            file.removeReference();
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
    public Future<MessageList> getMessages(String channel, int count, MessageFilterOptions options, MessageListAfterIdentifier after, ResponseCallback<MessageList> callback, ResponseErrorCallback errorCallback) {
        return executor.queue(() -> {
            MyMessageListAfterIdentifier a = (MyMessageListAfterIdentifier) after;
            long fileDateId = (a == null ? getDateIdentifier(new Date()) : a.fileDateId);
            SQLiteMessageStorageFile file = openFileFor(fileDateId, true);
            MessageQueryResult result = file.getMessages(channel, (a == null ? -1 : a.afterId), (a == null ? 0 : a.offset), count, options);
            file.removeReference();
            List<MessageInfo> ret = new ArrayList<>();
            if (result != null) {
                int afterId = result.getAfterId();
                if (result.getMessages().size() == count)
                    return new MessageList(result.getMessages(), afterId == -1 ? null : new MyMessageListAfterIdentifier(fileDateId, afterId, 0));
                ret.addAll(result.getMessages());
            }

            for (long i : availableFiles.tailSet(fileDateId - 1)) {
                file = openFileFor(i, true);
                result = file.getMessages(channel, -1, 0, count - ret.size(), options);
                file.removeReference();
                if (result != null) {
                    ret.addAll(0, result.getMessages());
                    int afterId = result.getAfterId();
                    if (ret.size() == count)
                        return new MessageList(ret, afterId == -1 ? null : new MyMessageListAfterIdentifier(i, afterId, 0));
                }
            }

            return new MessageList(ret, null);
        }, callback, errorCallback);
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

        long fileDateId;
        int afterId = 1;
        int offset = 0;

        MyMessageListAfterIdentifier() {
        }

        MyMessageListAfterIdentifier(long fileDateId, int afterId, int offset) {
            this.fileDateId = fileDateId;
            this.afterId = afterId;
            this.offset = offset;
        }


    }

}
