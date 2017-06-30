package io.mrarm.chatlib.android.storage;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import io.mrarm.chatlib.dto.MessageInfo;
import io.mrarm.chatlib.dto.MessageSenderInfo;
import io.mrarm.chatlib.dto.NickChangeMessageInfo;
import io.mrarm.chatlib.dto.NickPrefixList;

class MessageStorageHelper {

    private static final String PROP_BATCH = "batch";
    private static final String PROP_NICKCHANGE_NEWNICK = "newNick";


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
        MessageInfo.Builder builder = new MessageInfo.Builder(sender, text, type);
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
        return new MessageSenderInfo(nick, user, host, new NickPrefixList(prefixes), uuid);
    }

    static String serializeExtraData(MessageInfo info) {
        JsonObject object = new JsonObject();
        if (info.getBatch() != null)
            object.addProperty(PROP_BATCH, info.getBatch().getUUID().toString());
        if (info instanceof NickChangeMessageInfo) {
            NickChangeMessageInfo nickChangeMessage = ((NickChangeMessageInfo) info);
            object.addProperty(PROP_NICKCHANGE_NEWNICK, nickChangeMessage.getNewNick());
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
