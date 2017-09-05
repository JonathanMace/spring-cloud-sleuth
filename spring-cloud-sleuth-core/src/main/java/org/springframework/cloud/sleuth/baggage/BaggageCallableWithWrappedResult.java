package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.Callable;

public class BaggageCallableWithWrappedResult<T> implements Callable<WithBaggage<T>> {

	private final BaggageCallable<T> delegate;

	private BaggageCallableWithWrappedResult(BaggageCallable<T> delegate) {
		this.delegate = delegate;
	}

	@Override
	public WithBaggage<T> call() throws Exception {
		T result = this.delegate.call();
		return new WithBaggage<T>(result, this.delegate.getBaggageContext());
	}
	
	public static <V> BaggageCallableWithWrappedResult<V> wrap(Callable<V> delegate) {
		return new BaggageCallableWithWrappedResult<V>(BaggageCallable.wrap(delegate));
	}

}
