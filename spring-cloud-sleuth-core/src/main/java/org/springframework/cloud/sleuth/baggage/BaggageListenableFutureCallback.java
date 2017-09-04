package org.springframework.cloud.sleuth.baggage;

import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import brown.tracingplane.ActiveBaggage;
import brown.tracingplane.Baggage;
import brown.tracingplane.BaggageContext;

public class BaggageListenableFutureCallback<V> implements ListenableFutureCallback<V>, BaggageCarrier {
	
	private final SuccessCallback<V> successDelegate;
	private final FailureCallback failureDelegate;
	private final BaggageCarrier carrier;
	private final BaggageContext begin;
	private volatile BaggageContext end;
	
	public BaggageListenableFutureCallback(SuccessCallback<V> successDelegate, FailureCallback failureDelegate, BaggageCarrier carrier) {
		this.successDelegate = successDelegate;
		this.failureDelegate = failureDelegate;
		this.carrier = carrier;
		this.begin = ActiveBaggage.branch();
	}

	@Override
	public void onSuccess(V result) {
		BaggageContext precedingContext = ActiveBaggage.take();
		ActiveBaggage.set(begin);
		ActiveBaggage.join(Baggage.branch(carrier.getBaggageContext()));
		try {
			successDelegate.onSuccess(result);
		} finally {
			end = ActiveBaggage.take();
			ActiveBaggage.set(precedingContext);
		}
	}

	@Override
	public void onFailure(Throwable ex) {
		BaggageContext precedingContext = ActiveBaggage.take();
		ActiveBaggage.set(begin);
		ActiveBaggage.join(Baggage.branch(carrier.getBaggageContext()));
		try {
			failureDelegate.onFailure(ex);
		} finally {
			end = ActiveBaggage.take();
			ActiveBaggage.set(precedingContext);
		}
	}

	@Override
	public BaggageContext getBaggageContext() {
		return end;
	}

}
