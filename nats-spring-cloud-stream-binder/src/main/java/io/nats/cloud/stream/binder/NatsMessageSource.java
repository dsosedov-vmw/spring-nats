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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import io.nats.client.Message;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.messaging.support.GenericMessage;

/**
 * Message source for NATS connections, allowing synchronous polling.
 */
public class NatsMessageSource extends AbstractMessageSource<Object> implements Lifecycle {
	private static final Log logger = LogFactory.getLog(NatsMessageHandler.class);

	private NatsConsumerDestination destination;
	private StreamingConnection connection;
	private Subscription sub;

	/**
	 * Create a message source. Once started, the source will have a subscription but no threads.
	 * Calls to doReceive result in a nextMessage call at the NATS level. Currently nextMessage is
	 * called with Duration.ZERO and will wait forever.
	 * @param destination where to subscribe
	 * @param nc NATS connection
	 */
	public NatsMessageSource(NatsConsumerDestination destination, StreamingConnection nc) {
		this.destination = destination;
		this.connection = nc;
	}

	@Override
	protected Object doReceive() {
		if (this.sub == null) {
			return null;
		}

		logger.info("doReceive called...");

//		try {
//			Message m = this.sub.nextMessage(Duration.ZERO);
//
//			if (m != null) {
//				Map<String, Object> headers = new HashMap<>();
//				headers.put(NatsMessageProducer.SUBJECT, m.getSubject());
//				headers.put(NatsMessageProducer.REPLY_TO, m.getReplyTo());
//				GenericMessage<byte[]> gm = new GenericMessage<byte[]>(m.getData(), headers);
//				return gm;
//			}
//		}
//		catch (InterruptedException exp) {
//			logger.info("wait for message interrupted");
//		}

		return null;
	}

	@Override
	public boolean isRunning() {
		return this.sub != null;
	}

	@Override
	public void start() {
		if (this.sub != null) {
			return;
		}

		String sub = this.destination.getSubject();
		String queue = this.destination.getQueueGroup();

		try {
			if (queue != null && queue.length() > 0) {
				this.sub = this.connection.subscribe(sub, queue, new NatsStreamingMessageHandler(sub, this.connection), new SubscriptionOptions.Builder().deliverAllAvailable().build());
			}
			else {
				this.sub = this.connection.subscribe(sub, new NatsStreamingMessageHandler(sub, this.connection), new SubscriptionOptions.Builder().deliverAllAvailable().build());
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
		if (this.sub == null) {
			return;
		}

		try {
			this.sub.unsubscribe();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.sub = null;
	}

	@Override
	public String getComponentType() {
		return "nats:message-source";
	}
}
