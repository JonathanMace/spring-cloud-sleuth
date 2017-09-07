package org.springframework.cloud.sleuth.baggage;

import java.io.IOException;
import java.util.List;

import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.baggage.DetachedBaggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

public class BaggageResponseExtractor<T> implements ResponseExtractor<T>, BaggageCarrier {

	private static final XTraceLogger xtrace = XTrace.getLogger(BaggageResponseExtractor.class);

	private final ResponseExtractor<T> delegate;
	private final DetachedBaggage start;
	private volatile DetachedBaggage end;

	public BaggageResponseExtractor(ResponseExtractor<T> delegate) {
		this.delegate = delegate;
		this.start = Baggage.fork();
	}

	@Override
	public T extractData(ClientHttpResponse response) throws IOException {
		DetachedBaggage preceding = Baggage.swap(this.start);
		try {
			List<String> baggageHeaders = response.getHeaders().get("Baggage");
			if (baggageHeaders != null) {
				for (String baggageString : baggageHeaders) {
					Baggage.join(baggageString);
				}
			}
			xtrace.log("Receive response, extract data");
			return this.delegate.extractData(response);
		} finally {
			xtrace.log("Extract data done");
			this.end = Baggage.swap(preceding);
		}
	}

	@Override
	public DetachedBaggage getBaggageContext() {
		return end;
	}

}
