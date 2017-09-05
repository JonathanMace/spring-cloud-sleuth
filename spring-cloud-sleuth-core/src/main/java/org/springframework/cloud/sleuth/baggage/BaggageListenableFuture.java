package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import edu.brown.cs.systems.baggage.Baggage;


public class BaggageListenableFuture<V> extends BaggageFuture<V> implements ListenableFuture<V> {

	private final ListenableFuture<V> delegate;
	private final BaggageCarrier carrier;

	public BaggageListenableFuture(ListenableFuture<V> delegate, BaggageCarrier carrier) {
		super(delegate, carrier);
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

	@Override
	public void addCallback(ListenableFutureCallback<? super V> callback) {
		this.delegate.addCallback(new BaggageListenableFutureCallback<>(callback, callback, carrier));
	}

	@Override
	public void addCallback(SuccessCallback<? super V> successCallback, FailureCallback failureCallback) {
		this.delegate.addCallback(new BaggageListenableFutureCallback<>(successCallback, failureCallback, carrier));
	}

	public static <V> BaggageListenableFuture<V> wrap(ListenableFuture<V> future, BaggageCarrier carrier) {
		return new BaggageListenableFuture<V>(future, carrier);
	}

}
