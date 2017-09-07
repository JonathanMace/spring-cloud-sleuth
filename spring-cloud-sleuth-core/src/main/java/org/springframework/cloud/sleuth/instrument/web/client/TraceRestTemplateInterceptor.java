/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.cloud.sleuth.instrument.web.client;

import java.io.IOException;
import java.util.List;

import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanInjector;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.web.HttpTraceKeysInjector;
import org.springframework.cloud.sleuth.util.ExceptionUtils;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

/**
 * Interceptor that verifies whether the trance and span id has been set on the
 * request and sets them if one or both of them are missing.
 *
 * @author Marcin Grzejszczak
 * @author Spencer Gibb
 * @since 1.0.0
 *
 * @see org.springframework.web.client.RestTemplate
 */
public class TraceRestTemplateInterceptor extends AbstractTraceHttpRequestInterceptor
		implements ClientHttpRequestInterceptor {

	private static final XTraceLogger xtrace = XTrace.getLogger(TraceRestTemplateInterceptor.class);

	public TraceRestTemplateInterceptor(Tracer tracer, SpanInjector<HttpRequest> spanInjector,
			HttpTraceKeysInjector httpTraceKeysInjector) {
		super(tracer, spanInjector, httpTraceKeysInjector);
	}

	@Override
	public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
			throws IOException {
		xtrace.log("TraceRestTemplateInterceptor.intercept (enter)");
		publishStartEvent(request);
		try {
			ClientHttpResponse response = response(request, body, execution);
			List<String> baggageHeaders = response.getHeaders().get("Baggage");
			if (baggageHeaders != null) {
				xtrace.log("Received {} baggage headers in response",  baggageHeaders.size());
				for (String baggageHeader : baggageHeaders) {
					Baggage.join(baggageHeader);
				}
			} else {
				xtrace.log("Response baggage headers are null");
			}
			return response;
		} finally {
			xtrace.log("TraceRestTemplateInterceptor.intercept (return)");
		}
	}

	private ClientHttpResponse response(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
			throws IOException {
		try {
			return new TraceHttpResponse(this, execution.execute(request, body));
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Exception occurred while trying to execute the request. Will close the span ["
						+ currentSpan() + "]", e);
			}
			this.tracer.addTag(Span.SPAN_ERROR_TAG_NAME, ExceptionUtils.getExceptionMessage(e));
			this.tracer.close(currentSpan());
			throw e;
		}
	}

}
