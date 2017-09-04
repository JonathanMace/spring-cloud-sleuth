package org.springframework.cloud.sleuth.baggage;

import brown.tracingplane.ActiveBaggage;
import brown.tracingplane.BaggageContext;

public class BaggageRunnable implements Runnable, BaggageCarrier {

	private final Runnable delegate;
	private final BaggageContext begin;
	private volatile BaggageContext end;

	public BaggageRunnable(Runnable delegate) {
		this.delegate = delegate;
		this.begin = ActiveBaggage.branch();
	}

	@Override
	public void run() {
		BaggageContext precedingContext = ActiveBaggage.take();
		ActiveBaggage.set(this.begin);
		try {
			this.delegate.run();
		} finally {
			this.end = ActiveBaggage.take();
			ActiveBaggage.set(precedingContext);
		}
	}

	public void join() {
		ActiveBaggage.join(end);
	}

	@Override
	public BaggageContext getBaggageContext() {
		return end;
	}

}
