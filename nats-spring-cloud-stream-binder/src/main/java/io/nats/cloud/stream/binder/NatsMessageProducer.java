/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nats.cloud.stream.binder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import io.nats.client.Dispatcher;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.Lifecycle;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;

/**
 * MessageProducer for NATS connections.
 */
public class NatsMessageProducer implements MessageProducer, Lifecycle {
	private static final Log logger = LogFactory.getLog(NatsMessageProducer.class);

	/**
	 * The NATS subject for incoming message is stored in the SUBJECT header.
	 */
	public static final String SUBJECT = "subject";

	/**
	 * If an incoming message has a reply to subject, that will be stored in the REPLY_TO header for propagation to the NatsMessageSource.
	 */
	public static final String REPLY_TO = "reply_to";

	private NatsConsumerDestination destination;
	private StreamingConnection connection;
	private MessageChannel output;
	private Dispatcher dispatcher;

	/**
	 * Create a message producer. Once started the producer will use a dispatcher, and the associated thread, to
	 * listen for and handle incoming messages.
	 * @param destination where to subscribe
	 * @param nc NATS connection
	 */
	public NatsMessageProducer(NatsConsumerDestination destination, StreamingConnection nc) {
		this.destination = destination;
		this.connection = nc;
	}

	@Override
	public void setOutputChannel(MessageChannel outputChannel) {
		this.output = outputChannel;
	}

	@Override
	public MessageChannel getOutputChannel() {
		return this.output;
	}

	@Override
	public boolean isRunning() {
		return this.dispatcher != null;
	}

	@Override
	public void start() {
		String sub = this.destination.getSubject();
		String queue = this.destination.getQueueGroup();

		try {
			if (queue != null && queue.length() > 0) {
				this.connection.subscribe(sub, queue, new NatsStreamingMessageHandler(this.output), new SubscriptionOptions.Builder().deliverAllAvailable().build());
			}
			else {
				this.connection.subscribe(sub, new NatsStreamingMessageHandler(this.output), new SubscriptionOptions.Builder().deliverAllAvailable().build());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		if (this.dispatcher == null) {
			return;
		}

		this.connection.getNatsConnection().closeDispatcher(this.dispatcher);
		this.dispatcher = null;
	}
}
