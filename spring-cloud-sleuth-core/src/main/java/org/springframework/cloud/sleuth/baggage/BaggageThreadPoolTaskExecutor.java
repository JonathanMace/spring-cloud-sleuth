package org.springframework.cloud.sleuth.baggage;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;

@SuppressWarnings("serial")
public class BaggageThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {
	
	private final ThreadPoolTaskExecutor delegate;

	public BaggageThreadPoolTaskExecutor(ThreadPoolTaskExecutor delegate) {
		this.delegate = delegate;
	}

	@Override
	public void execute(Runnable task) {
		this.delegate.execute(new BaggageRunnable(task));
	}

	@Override
	public void execute(Runnable task, long startTimeout) {
		this.delegate.execute(new BaggageRunnable(task), startTimeout);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Future<?> submit(Runnable task) {
		BaggageRunnable wrappedTask = new BaggageRunnable(task);
		Future<?> future = this.delegate.submit(wrappedTask);
		if (future instanceof ListenableFuture) {
			return new BaggageListenableFuture<>((ListenableFuture) future, wrappedTask);
		} else {
			return new BaggageFuture<>(future, wrappedTask);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T> Future<T> submit(Callable<T> task) {
		BaggageCallable<T> wrappedTask = new BaggageCallable<T>(task);
		Future<T> future = this.delegate.submit(wrappedTask);
		if (future instanceof ListenableFuture) {
			return new BaggageListenableFuture<>((ListenableFuture) future, wrappedTask);
		} else {
			return new BaggageFuture<>(future, wrappedTask);
		}
	}

	@Override
	public ListenableFuture<?> submitListenable(Runnable task) {
		BaggageRunnable wrappedTask = new BaggageRunnable(task);
		return new BaggageListenableFuture<>(this.delegate.submitListenable(wrappedTask), wrappedTask);
	}

	@Override
	public <T> ListenableFuture<T> submitListenable(Callable<T> task) {
		BaggageCallable<T> wrappedTask = new BaggageCallable<T>(task);
		return new BaggageListenableFuture<>(this.delegate.submitListenable(wrappedTask), wrappedTask);
	}

	@Override
	public ThreadPoolExecutor getThreadPoolExecutor() throws IllegalStateException {
		return this.delegate.getThreadPoolExecutor();
	}

	public void destroy() {
		this.delegate.destroy();
		super.destroy();
	}

	@Override
	public void afterPropertiesSet() {
		this.delegate.afterPropertiesSet();
		super.afterPropertiesSet();
	}

}
