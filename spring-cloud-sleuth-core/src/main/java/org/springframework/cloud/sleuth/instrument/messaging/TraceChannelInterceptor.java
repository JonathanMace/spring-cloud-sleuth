/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.sleuth.instrument.messaging;

import org.springframework.cloud.sleuth.Log;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanExtractor;
import org.springframework.cloud.sleuth.SpanInjector;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.baggage.ZipkinBaggage;
import org.springframework.cloud.sleuth.sampler.NeverSampler;
import org.springframework.cloud.sleuth.util.ExceptionUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;

import com.google.common.io.BaseEncoding;

import edu.brown.cs.systems.tracingplane.transit_layer.Baggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

/**
 * A channel interceptor that automatically starts / continues / closes and
 * detaches spans.
 *
 * @author Dave Syer
 * @since 1.0.0
 */
public class TraceChannelInterceptor extends AbstractTraceChannelInterceptor {

	private static final XTraceLogger XTRACE = XTrace.getLogger(TraceChannelInterceptor.class);

	public TraceChannelInterceptor(Tracer tracer, TraceKeys traceKeys, SpanExtractor<Message<?>> spanExtractor,
			SpanInjector<MessageBuilder<?>> spanInjector) {
		super(tracer, traceKeys, spanExtractor, spanInjector);
	}

	@Override
	public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {
		XTRACE.log("afterSendCompletion {}", message);
		Span spanFromHeader = getSpanFromHeader(message);
		if (containsServerReceived(spanFromHeader)) {
			spanFromHeader.logEvent(Span.SERVER_SEND);
		} else if (spanFromHeader != null) {
			spanFromHeader.logEvent(Span.CLIENT_RECV);
		}
		addErrorTag(ex);
		getTracer().close(spanFromHeader);
	}

	private boolean containsServerReceived(Span span) {
		if (span == null) {
			return false;
		}
		for (Log log : span.logs()) {
			if (Span.SERVER_RECV.equals(log.getEvent())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {
		XTRACE.log("preSend {} (enter)", message);
		Span parentSpan = getTracer().isTracing() ? getTracer().getCurrentSpan() : buildSpan(message);
		String name = getMessageChannelName(channel);
		Span span = startSpan(parentSpan, name, message);
		MessageBuilder<?> messageBuilder = MessageBuilder.fromMessage(message);
		// Backwards compatibility
		if (message.getHeaders().containsKey(TraceMessageHeaders.OLD_MESSAGE_SENT_FROM_CLIENT)
				|| message.getHeaders().containsKey(TraceMessageHeaders.MESSAGE_SENT_FROM_CLIENT)) {
			span.logEvent(Span.SERVER_RECV);
		} else {
			span.logEvent(Span.CLIENT_SEND);
			// Backwards compatibility
			messageBuilder.setHeader(TraceMessageHeaders.OLD_MESSAGE_SENT_FROM_CLIENT, true);
			messageBuilder.setHeader(TraceMessageHeaders.MESSAGE_SENT_FROM_CLIENT, true);
		}
		getSpanInjector().inject(span, messageBuilder);
		MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
		headers.copyHeaders(messageBuilder.build().getHeaders());

		headers.setHeader(TraceMessageHeaders.BAGGAGE_NAME,
				BaseEncoding.base64().encode(Baggage.serialize(Baggage.branch())));

		GenericMessage<Object> returnMessage = new GenericMessage<Object>(message.getPayload(),
				headers.getMessageHeaders());
		XTRACE.log("preSend {} (return) {}", message, returnMessage);
		return returnMessage;
	}

	@Override
	public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
		XTRACE.log("postSend (enter) {}", message);
		super.postSend(message, channel, sent);
		XTRACE.log("postSend (return) {}", message);
	}

	private Span startSpan(Span span, String name, Message<?> message) {
		if (span != null) {
			return getTracer().createSpan(name, span);
		}
		// Backwards compatibility
		ZipkinBaggage zb = ZipkinBaggage.get();
		if (zb != null && zb.sampled != null && zb.sampled == false) {
			return getTracer().createSpan(name, NeverSampler.INSTANCE);
		}
		return getTracer().createSpan(name);
	}

	@Override
	public Message<?> beforeHandle(Message<?> message, MessageChannel channel, MessageHandler handler) {
		XTRACE.log("beforeHandle (enter) {}", message);
		Span spanFromHeader = getSpanFromHeader(message);
		if (spanFromHeader != null) {
			spanFromHeader.logEvent(Span.SERVER_RECV);
		}
		getTracer().continueSpan(spanFromHeader);
		XTRACE.log("beforeHandle (return) {}", message);
		return message;
	}

	@Override
	public void afterMessageHandled(Message<?> message, MessageChannel channel, MessageHandler handler, Exception ex) {
		XTRACE.log("afterMessageHandled (enter) {}", message);
		Span spanFromHeader = getSpanFromHeader(message);
		if (spanFromHeader != null) {
			spanFromHeader.logEvent(Span.SERVER_SEND);
			addErrorTag(ex);
		}
		getTracer().detach(spanFromHeader);
		XTRACE.log("afterMessageHandled (return) {}", message);
	}

	private void addErrorTag(Exception ex) {
		if (ex != null) {
			getTracer().addTag(Span.SPAN_ERROR_TAG_NAME, ExceptionUtils.getExceptionMessage(ex));
		}
	}

	private Span getSpanFromHeader(Message<?> message) {
		if (message == null) {
			return null;
		}
		Object object = message.getHeaders().get(TraceMessageHeaders.OLD_SPAN_HEADER);
		if (object instanceof Span) {
			return (Span) object;
		}
		object = message.getHeaders().get(TraceMessageHeaders.SPAN_HEADER);
		if (object instanceof Span) {
			return (Span) object;
		}
		return null;
	}

	public boolean preReceive(MessageChannel channel) {
		XTRACE.log("preReceive (enter) {}", channel);
		try {
			return super.preReceive(channel);
		} finally {
			XTRACE.log("preReceive (return) {}", channel);
		}
	}

	@Override
	public Message<?> postReceive(Message<?> message, MessageChannel channel) {
		XTRACE.log("postReceive (enter) {} {}", message, channel);
		try {
			return super.postReceive(message, channel);
		} finally {
			XTRACE.log("postReceive (return) {} {}", message, channel);
		}
	}

	@Override
	public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
		XTRACE.log("afterReceiveCompletion (enter) {} {}", message, channel);
		try {
			super.afterReceiveCompletion(message, channel, ex);
		} finally {
			XTRACE.log("afterReceiveCompletion (return) {} {}", message, channel);
		}
	}

}
