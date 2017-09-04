package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import brown.tracingplane.ActiveBaggage;

public class BaggageFuture<V> implements Future<V> {
	
	private final Future<V> delegate;
	private final BaggageCarrier carrier;

	public BaggageFuture(Future<V> delegate, BaggageCarrier carrier) {
		this.delegate = delegate;
		this.carrier = carrier;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return this.delegate.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return this.delegate.isCancelled();
	}

	@Override
	public boolean isDone() {
		return this.delegate.isDone();
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		V returnValue = this.delegate.get();
		ActiveBaggage.join(carrier.getBaggageContext());
		return returnValue;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		V returnValue = this.delegate.get(timeout, unit);
		ActiveBaggage.join(carrier.getBaggageContext());
		return returnValue;
	}

}
