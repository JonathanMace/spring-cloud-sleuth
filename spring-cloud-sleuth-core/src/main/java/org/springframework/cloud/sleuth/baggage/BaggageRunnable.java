package org.springframework.cloud.sleuth.baggage;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.baggage.DetachedBaggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

public class BaggageRunnable implements Runnable, BaggageCarrier {

	private static final XTraceLogger XTRACE = XTrace.getLogger(BaggageRunnable.class);

	private final Runnable delegate;
	private final DetachedBaggage begin;
	private volatile DetachedBaggage end;

	private BaggageRunnable(Runnable delegate) {
		this.delegate = delegate;
		this.begin = Baggage.fork();
	}

	@Override
	public void run() {
		DetachedBaggage precedingContext = Baggage.swap(this.begin);
		XTRACE.log("BaggageRunnable.run");
		try {
			this.delegate.run();
		} finally {
			XTRACE.log("BaggageRunnable.run done");
			this.end = Baggage.swap(precedingContext);
		}
	}

	public void join() {
		Baggage.join(end);
	}

	@Override
	public DetachedBaggage getBaggageContext() {
		return end;
	}

	public static BaggageRunnable wrap(Runnable runnable) {
		if (runnable instanceof BaggageRunnable) {
			return (BaggageRunnable) runnable;
		} else {
			return new BaggageRunnable(runnable);
		}
	}

}
