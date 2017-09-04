package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.Executor;

public class BaggageExecutor implements Executor {

	private Executor delegate;

	public BaggageExecutor(Executor delegate) {
		this.delegate = delegate;
	}

	@Override
	public void execute(Runnable command) {
		if (!(command instanceof BaggageRunnable)) {
			command = new BaggageRunnable(command);
		}
		this.delegate.execute(command);
	}

}
