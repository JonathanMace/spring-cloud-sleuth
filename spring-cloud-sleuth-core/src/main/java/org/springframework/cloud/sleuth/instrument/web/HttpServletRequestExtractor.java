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

package org.springframework.cloud.sleuth.instrument.web;

import java.lang.invoke.MethodHandles;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Span.SpanBuilder;
import org.springframework.cloud.sleuth.SpanExtractor;
import org.springframework.cloud.sleuth.baggage.ZipkinBaggage;
import org.springframework.web.util.UrlPathHelper;

/**
 * Creates a {@link SpanBuilder} from {@link HttpServletRequest}
 *
 * @author Marcin Grzejszczak
 *
 * @since 1.0.0
 */
class HttpServletRequestExtractor implements SpanExtractor<HttpServletRequest> {

	private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
	private static final String HTTP_COMPONENT = "http";

	private final Pattern skipPattern;

	private UrlPathHelper urlPathHelper = new UrlPathHelper();

	public HttpServletRequestExtractor(Pattern skipPattern) {
		this.skipPattern = skipPattern;
	}

	@Override
	public Span joinTrace(HttpServletRequest carrier) {
		ZipkinBaggage zb = ZipkinBaggage.get();
		if (zb == null || zb.traceID == null) {
			// can't build a Span without trace id
			return null;
		}
		try {
			String uri = this.urlPathHelper.getPathWithinApplication(carrier);
			boolean skip = this.skipPattern.matcher(uri).matches()
					|| (zb.sampled != null && zb.sampled == false);
			long spanId = spanId(zb);
			return buildParentSpan(zb, carrier, uri, skip, spanId);
		} catch (Exception e) {
			log.error("Exception occurred while trying to extract span from carrier", e);
			return null;
		}
	}

	private long spanId(ZipkinBaggage zb) {
		if (zb.spanID == null) {
			if (log.isDebugEnabled()) {
				log.debug("Request is missing a span id but it has a trace id. We'll assume that this is "
						+ "a root span with span id equal to the lower 64-bits of the trace id");
			}
			return zb.traceID;
		} else {
			return zb.spanID;
		}
	}

	private Span buildParentSpan(ZipkinBaggage zb, HttpServletRequest carrier, String uri, boolean skip, long spanId) {
		SpanBuilder span = Span.builder()
			.traceId(zb.traceID)
			.spanId(spanId);
		// BAGGAGE TODO: Could update ZipkinBaggage to include process id and span name name, omitting for now
//		String processId = carrier.getHeader(Span.PROCESS_ID_NAME);
//		String parentName = carrier.getHeader(Span.SPAN_NAME_NAME);
//		if (StringUtils.hasText(parentName)) {
//			span.name(parentName);
//		}
//		else {
			span.name(HTTP_COMPONENT + ":/parent" + uri);
//		}
//		if (StringUtils.hasText(processId)) {
//			span.processId(processId);
//		}
		if (zb.parentSpanID != null) {
			span.parent(zb.parentSpanID);
		}
		span.remote(true);
		if (skip) {
			span.exportable(false);
		}
		
		return span.build();
	}
}
