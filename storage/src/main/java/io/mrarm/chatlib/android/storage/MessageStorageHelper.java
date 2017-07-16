package io.mrarm.chatlib.android.storage;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import io.mrarm.chatlib.dto.ChannelModeMessageInfo;
import io.mrarm.chatlib.dto.MessageInfo;
import io.mrarm.chatlib.dto.MessageSenderInfo;
import io.mrarm.chatlib.dto.NickChangeMessageInfo;
import io.mrarm.chatlib.dto.NickPrefixList;

class MessageStorageHelper {

    private static final String PROP_BATCH = "batch";
    private static final String PROP_NICKCHANGE_NEWNICK = "newNick";
    private static final String PROP_CHANNELMODE_ENTRIES = "entries";


    private static final Gson gson = new Gson();

    static MessageInfo deserializeMessage(MessageSenderInfo sender, Date date, String text,
                                                 int typeInt, String extraData) {
        MessageInfo.MessageType type = MessageInfo.MessageType.NORMAL;
        for (MessageInfo.MessageType t : MessageInfo.MessageType.values()) {
            if (t.asInt() == typeInt)
                type = t;
        }
        JsonObject o = gson.fromJson(extraData, JsonObject.class);
        if (type == MessageInfo.MessageType.NICK_CHANGE)
            return new NickChangeMessageInfo(sender, date, o.get(PROP_NICKCHANGE_NEWNICK).getAsString());
        MessageInfo.Builder builder;
        if (type == MessageInfo.MessageType.MODE) {
            JsonArray entriesArray = o.get(PROP_CHANNELMODE_ENTRIES).getAsJsonArray();
            List<ChannelModeMessageInfo.Entry> entries = new ArrayList<>(entriesArray.size());
            for (JsonElement e : entriesArray)
                entries.add(gson.fromJson(e.getAsJsonObject(), ChannelModeMessageInfo.Entry.class));
            builder = new ChannelModeMessageInfo.Builder(sender, entries);
        } else {
            builder = new MessageInfo.Builder(sender, text, type);
        }
        builder.setDate(date);
        if (o.has(PROP_BATCH)) {
            // TODO: find the batch
        }
        return builder.build();
    }

    static String serializeSenderInfo(MessageSenderInfo sender) {
        return (sender.getNickPrefixes() == null ? "" : sender.getNickPrefixes().toString()) + " " + sender.getNick() + "!" + sender.getUser() + "@" + sender.getHost();
    }

    static MessageSenderInfo deserializeSenderInfo(String serialized, UUID uuid) {
        int iof = serialized.indexOf(' ');
        String prefixes = serialized.substring(0, iof);
        int iof2 = serialized.indexOf('!', iof + 1);
        String nick = serialized.substring(iof + 1, iof2);
        iof = serialized.indexOf('@', iof2 + 1);
        String user = serialized.substring(iof2 + 1, iof);
        String host = serialized.substring(iof + 1);
        return new MessageSenderInfo(nick, user, host, prefixes.length() > 0 ? new NickPrefixList(prefixes) : null, uuid);
    }

    static String serializeExtraData(MessageInfo info) {
        JsonObject object = new JsonObject();
        if (info.getBatch() != null)
            object.addProperty(PROP_BATCH, info.getBatch().getUUID().toString());
        if (info instanceof NickChangeMessageInfo) {
            NickChangeMessageInfo nickChangeMessage = ((NickChangeMessageInfo) info);
            object.addProperty(PROP_NICKCHANGE_NEWNICK, nickChangeMessage.getNewNick());
        }
        if (info instanceof ChannelModeMessageInfo) {
            ChannelModeMessageInfo modeMessage = ((ChannelModeMessageInfo) info);
            object.add(PROP_CHANNELMODE_ENTRIES, gson.toJsonTree(modeMessage.getEntries()));
        }
        return gson.toJson(object);
    }


    static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer b = ByteBuffer.wrap(new byte[16]);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return b.array();
    }

    static UUID bytesToUUID(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        return new UUID(b.getLong(), b.getLong());
    }

}
