package io.mrarm.chatlib.android.storage;

import java.util.List;

import io.mrarm.chatlib.dto.MessageInfo;

class MessageQueryResult {

    private List<MessageInfo> messages;
    private int afterId;

    public MessageQueryResult(List<MessageInfo> messages, int afterId) {
        this.messages = messages;
        this.afterId = afterId;
    }

    public List<MessageInfo> getMessages() {
        return messages;
    }

    public int getAfterId() {
        return afterId;
    }

}
