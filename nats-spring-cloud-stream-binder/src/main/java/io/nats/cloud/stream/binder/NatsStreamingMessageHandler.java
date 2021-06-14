package io.nats.cloud.stream.binder;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.StreamingConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static io.nats.cloud.stream.binder.NatsMessageProducer.REPLY_TO;
import static io.nats.cloud.stream.binder.NatsMessageProducer.SUBJECT;

public class NatsStreamingMessageHandler implements MessageHandler {
    private static final Log logger = LogFactory.getLog(NatsStreamingMessageHandler.class);

    private MessageChannel output;

    public NatsStreamingMessageHandler(MessageChannel output) {
        this.output = output;
    }

    @Override
    public void onMessage(Message message) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(SUBJECT, message.getSubject());
        headers.put(REPLY_TO, message.getReplyTo());
        GenericMessage<byte[]> m = new GenericMessage<byte[]>(message.getData(), headers);
        this.output.send(m);
    }
}
