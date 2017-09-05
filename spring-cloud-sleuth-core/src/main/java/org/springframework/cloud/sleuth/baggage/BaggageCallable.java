package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.Callable;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.baggage.DetachedBaggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

public class BaggageCallable<V> implements Callable<V>, BaggageCarrier {

	private static final XTraceLogger XTRACE = XTrace.getLogger(BaggageCallable.class);

	private final Callable<V> delegate;
	private final DetachedBaggage begin;
	private volatile DetachedBaggage end;

	protected BaggageCallable(Callable<V> delegate) {
		this.delegate = delegate;
		this.begin = Baggage.fork();
	}

	@Override
	public V call() throws Exception {
		XTRACE.log("Removing previous context for {} {}", this, delegate);
		DetachedBaggage precedingContext = Baggage.swap(this.begin);
		XTRACE.log("BaggageCallable.call {} {}", this, delegate);
		try {
			return this.delegate.call();
		} finally {
			XTRACE.log("BaggageCallable.call done {} {}", this, delegate);
			this.end = Baggage.swap(precedingContext);
			XTRACE.log("Reinstating previous context for {} {}", this, delegate);
		}
	}

	@Override
	public DetachedBaggage getBaggageContext() {
		return end;
	}

	public static <V> BaggageCallable<V> wrap(Callable<V> callable) {
		if (callable instanceof BaggageCallable) {
			return (BaggageCallable<V>) callable;
		} else {
			return new BaggageCallable<V>(callable);
		}
	}

}
