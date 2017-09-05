package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.util.concurrent.ListenableFuture;

import edu.brown.cs.systems.baggage.Baggage;

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
		Baggage.join(carrier.getBaggageContext());
		return returnValue;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		V returnValue = this.delegate.get(timeout, unit);
		Baggage.join(carrier.getBaggageContext());
		return returnValue;
	}
	
	public static <V> BaggageFuture<V> wrap(Future<V> future, BaggageCarrier carrier) {
		if (future instanceof ListenableFuture) {
			return BaggageListenableFuture.wrap((ListenableFuture<V>) future, carrier);
		} else {
			return new BaggageFuture<V>(future, carrier);
		}
	}

}
