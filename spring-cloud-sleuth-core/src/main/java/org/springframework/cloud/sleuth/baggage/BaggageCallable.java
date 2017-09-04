package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.Callable;

import brown.tracingplane.ActiveBaggage;
import brown.tracingplane.BaggageContext;

public class BaggageCallable<V> implements Callable<V>, BaggageCarrier {

	private final Callable<V> delegate;
	private final BaggageContext begin;
	private volatile BaggageContext end;
	
	public BaggageCallable(Callable<V> delegate) {
		this.delegate = delegate;
		this.begin = ActiveBaggage.branch();
	}

	@Override
	public V call() throws Exception {
		BaggageContext precedingContext = ActiveBaggage.take();
		ActiveBaggage.set(this.begin);
		try {
			return this.delegate.call();
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
