package io.mrarm.chatlib.android.storage;

import android.os.Handler;

import io.mrarm.chatlib.ResponseCallback;
import io.mrarm.chatlib.ResponseErrorCallback;
import io.mrarm.chatlib.dto.MessageFilterOptions;
import io.mrarm.chatlib.dto.MessageId;
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

    private static final MyMessageIdParser MESSAGE_ID_PARSER = new MyMessageIdParser();

    private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("'messages-'yyyy-MM-dd'.db'", Locale.getDefault());

    private final Handler handler = new Handler();
    private final SimpleRequestExecutor executor = new SimpleRequestExecutor();
    private final List<MessageListener> globalListeners = new ArrayList<>();
    private final Map<String, List<MessageListener>> listeners = new HashMap<>();
    final Map<Long, SQLiteMessageStorageFile> files = new HashMap<>();
    private final SortedSet<Long> availableFilesAsc = new TreeSet<>();
    private final SortedSet<Long> availableFilesDesc = new TreeSet<>(Collections.reverseOrder());
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
            if (!readOnly) {
                availableFilesAsc.add(dateId);
                availableFilesDesc.add(dateId);
            }
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
            availableFilesAsc.clear();
            availableFilesDesc.clear();
            for (File child : directory.listFiles()) {
                if (child.isFile()) {
                    try {
                        long dateId = getDateIdFromFileName(child.getName());
                        availableFilesAsc.add(dateId);
                        availableFilesDesc.add(dateId);
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
            Date d = new Date();
            SQLiteMessageStorageFile file = openFileFor(d, false);
            long msgId = file.addMessage(channel, messageInfo);
            MessageId mgsIdObj = new MyMessageId(getDateIdentifier(d), (int) msgId);
            file.removeReference();
            synchronized (listeners) {
                for (MessageListener listener : globalListeners)
                    listener.onMessage(channel, messageInfo, mgsIdObj);
                if (listeners.containsKey(channel)) {
                    for (MessageListener listener : listeners.get(channel))
                        listener.onMessage(channel, messageInfo, mgsIdObj);
                }
            }
            return null;
        }, callback, errorCallback);
    }

    private MessageList getMessagesImpl(String channel, int count, MessageFilterOptions options, MessageListAfterIdentifier after) {
        MyMessageListOlderIdentifier a = (MyMessageListOlderIdentifier) after;
        boolean isBefore = a instanceof MyMessageListNewerIdentifier;
        MyMessageListNewerIdentifier newerId = null;
        if (after != null)
            newerId = new MyMessageListNewerIdentifier(a.fileDateId, a.afterId, a.offset);
        long fileDateId = (a == null ? getDateIdentifier(new Date()) : a.fileDateId);
        SQLiteMessageStorageFile file = openFileFor(fileDateId, true);
        MessageQueryResult result = file.getMessages(channel, (a == null ? -1 : a.afterId), (a == null ? 0 : a.offset), count, isBefore, options);
        file.removeReference();
        List<MessageInfo> ret = new ArrayList<>();
        if (result != null) {
            int afterId = result.getAfterId();
            if (result.getMessages().size() == count)
                return new MessageList(result.getMessages(), newerId, afterId == -1 ? null : new MyMessageListOlderIdentifier(fileDateId, afterId, 0));
            ret.addAll(result.getMessages());
        }

        for (long i : (isBefore ? availableFilesAsc.tailSet(fileDateId + 1) : availableFilesDesc.tailSet(fileDateId - 1))) {
            file = openFileFor(i, true);
            result = file.getMessages(channel, -1, 0, count - ret.size(), isBefore, options);
            file.removeReference();
            if (result != null) {
                if (isBefore)
                    ret.addAll(result.getMessages());
                else
                    ret.addAll(0, result.getMessages());
                int afterId = result.getAfterId();
                if (ret.size() == count)
                    return new MessageList(ret, newerId, afterId == -1 ? null : new MyMessageListOlderIdentifier(i, afterId, 0));
            }
        }

        return new MessageList(ret, newerId, null);
    }

    @Override
    public Future<MessageList> getMessages(String channel, int count, MessageFilterOptions options, MessageListAfterIdentifier after, ResponseCallback<MessageList> callback, ResponseErrorCallback errorCallback) {
        return executor.queue(() -> getMessagesImpl(channel, count, options, after), callback, errorCallback);
    }

    @Override
    public Future<MessageList> getMessagesNear(String s, MessageId messageId, MessageFilterOptions filter, ResponseCallback<MessageList> callback, ResponseErrorCallback errorCallback) {
        return executor.queue(() -> {
            if (!(messageId instanceof MyMessageId))
                throw new RuntimeException("Invalid message id type");
            MyMessageId m = (MyMessageId) messageId;
            MessageList older = getMessagesImpl(s, 50, filter, new MyMessageListOlderIdentifier(m.fileDateId, m.id, 0));
            MessageList newer = getMessagesImpl(s, 50, filter, new MyMessageListNewerIdentifier(m.fileDateId, m.id - 1 /* include the current message */, 0));
            List<MessageInfo> ret = older.getMessages(); // we can mutate it just fine, as we control the object
            ret.addAll(newer.getMessages());
            return new MessageList(ret, newer.getNewer(), older.getOlder());
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

    @Override
    public MessageId.Parser getMessageIdParser() {
        return getMessageIdParserInstance();
    }

    static class MyMessageId implements MessageId {

        long fileDateId;
        int id;

        MyMessageId(long fileDateId, int id) {
            this.fileDateId = fileDateId;
            this.id = id;
        }

        @Override
        public String toString() {
            return fileDateId + ":" + id;
        }
    }

    static class MyMessageIdParser implements MessageId.Parser {

        @Override
        public MessageId parse(String str) {
            int i = str.indexOf(':');
            long fileDateId = Long.parseLong(str.substring(0, i));
            long id = Long.parseLong(str.substring(i + 1));
            return new MyMessageId(fileDateId, (int) id);
        }

    }

    static class MyMessageListOlderIdentifier implements MessageListAfterIdentifier {

        long fileDateId;
        int afterId = 1;
        int offset = 0;

        MyMessageListOlderIdentifier(long fileDateId, int afterId, int offset) {
            this.fileDateId = fileDateId;
            this.afterId = afterId;
            this.offset = offset;
        }

    }


    static class MyMessageListNewerIdentifier extends MyMessageListOlderIdentifier {
        MyMessageListNewerIdentifier(long fileDateId, int afterId, int offset) {
            super(fileDateId, afterId, offset);
        }
    }

    public static MyMessageIdParser getMessageIdParserInstance() {
        return MESSAGE_ID_PARSER;
    }

}
