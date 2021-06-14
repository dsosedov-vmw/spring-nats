package io.nats.cloud.stream.binder;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.StreamingConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class NatsStreamingMessageHandler implements MessageHandler {
    private static final Log logger = LogFactory.getLog(NatsStreamingMessageHandler.class);

    private String subject;
    private StreamingConnection connection;

    public NatsStreamingMessageHandler(String subject, StreamingConnection nc) {
        this.subject = subject;
        this.connection = nc;
    }

    @Override
    public void onMessage(Message message) {
        Object payload = message.getData();
        byte[] bytes = null;

        if (payload instanceof byte[]) {
            bytes = (byte[]) payload;
        }
        else if (payload instanceof ByteBuffer) {
            ByteBuffer buf = ((ByteBuffer) payload);
            bytes = new byte[buf.remaining()];
            buf.get(bytes);
        }
        else if (payload instanceof String) {
            bytes = ((String) payload).getBytes(StandardCharsets.UTF_8);
        }

        if (bytes == null) {
            logger.warn("NATS handler only supports byte array, byte buffer and string messages");
            return;
        }

        //Object rt = message.getReplyTo();
        String replyTo = message.getReplyTo(); //rt != null ? rt.toString() : null;
        String subj = replyTo != null && !replyTo.isEmpty() ? replyTo : this.subject;

        if (this.connection != null && subj != null && subj.length() > 0) {
            try {
                this.connection.publish(subj, bytes);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}
