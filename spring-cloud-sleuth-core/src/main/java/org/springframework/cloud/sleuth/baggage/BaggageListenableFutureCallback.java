package org.springframework.cloud.sleuth.baggage;

import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.baggage.DetachedBaggage;


public class BaggageListenableFutureCallback<V> implements ListenableFutureCallback<V>, BaggageCarrier {
	
	private final SuccessCallback<V> successDelegate;
	private final FailureCallback failureDelegate;
	private final BaggageCarrier carrier;
	private final DetachedBaggage begin;
	private volatile DetachedBaggage end;
	
	public BaggageListenableFutureCallback(SuccessCallback<V> successDelegate, FailureCallback failureDelegate, BaggageCarrier carrier) {
		this.successDelegate = successDelegate;
		this.failureDelegate = failureDelegate;
		this.carrier = carrier;
		this.begin = Baggage.fork();
	}

	@Override
	public void onSuccess(V result) {
		DetachedBaggage precedingContext = Baggage.swap(this.begin);
		Baggage.join(DetachedBaggage.split(carrier.getBaggageContext()));
		try {
			successDelegate.onSuccess(result);
		} finally {
			this.end = Baggage.swap(precedingContext);
		}
	}

	@Override
	public void onFailure(Throwable ex) {
		DetachedBaggage precedingContext = Baggage.swap(this.begin);
		Baggage.join(DetachedBaggage.split(carrier.getBaggageContext()));
		try {
			failureDelegate.onFailure(ex);
		} finally {
			this.end = Baggage.swap(precedingContext);
		}
	}

	@Override
	public DetachedBaggage getBaggageContext() {
		return end;
	}

}
