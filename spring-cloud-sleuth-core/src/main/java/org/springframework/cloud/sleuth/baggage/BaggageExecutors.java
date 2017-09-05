package org.springframework.cloud.sleuth.baggage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.xtrace.XTrace;
import edu.brown.cs.systems.xtrace.logging.XTraceLogger;

public class BaggageExecutors {

	public static Executor wrap(Executor delegate) {
		if (delegate instanceof AsyncListenableTaskExecutor) {
			return new BaggageAsyncListenableTaskExecutor<AsyncListenableTaskExecutor>(
					(AsyncListenableTaskExecutor) delegate);
		} else if (delegate instanceof AsyncTaskExecutor) {
			return new BaggageAsyncTaskExecutor<AsyncTaskExecutor>((AsyncTaskExecutor) delegate);
		} else if (delegate instanceof ExecutorService) {
			return new BaggageExecutorService<ExecutorService>((ExecutorService) delegate);
		} else {
			return new BaggageExecutor<Executor>(delegate);
		}
	}

	static class BaggageExecutor<E extends Executor> implements TaskExecutor {

		final static XTraceLogger xtrace = XTrace.getLogger(BaggageExecutor.class);

		protected E delegate;

		public BaggageExecutor(E delegate) {
			this.delegate = delegate;
		}

		@Override
		public void execute(Runnable command) {
			xtrace.log("Executing Runnable in BaggageExecutor: {}", String.valueOf(command));
			if (!(command instanceof BaggageRunnable)) {
				command = BaggageRunnable.wrap(command);
			}
			this.delegate.execute(command);
		}

	}

	static class BaggageAsyncTaskExecutor<E extends AsyncTaskExecutor> extends BaggageExecutor<E>
			implements AsyncTaskExecutor {

		final static XTraceLogger xtrace = XTrace.getLogger(BaggageAsyncTaskExecutor.class);

		public BaggageAsyncTaskExecutor(E delegate) {
			super(delegate);
		}

		@Override
		public void execute(Runnable task, long startTimeout) {
			xtrace.log("execute runnable {}", task);
			this.delegate.execute(BaggageRunnable.wrap(task), startTimeout);
		}

		@Override
		public Future<?> submit(Runnable task) {
			xtrace.log("submit runnable {}", task);
			BaggageRunnable baggageTask = BaggageRunnable.wrap(task);
			return BaggageFuture.wrap(this.delegate.submit(baggageTask), baggageTask);
		}

		@Override
		public <T> Future<T> submit(Callable<T> task) {
			xtrace.log("submit Callable {}", task);
			BaggageCallable<T> baggageTask = BaggageCallable.wrap(task);
			return BaggageFuture.wrap(this.delegate.submit(baggageTask), baggageTask);
		}

		@Override
		public void execute(Runnable task) {
			this.delegate.execute(BaggageRunnable.wrap(task));
		}
	}

	static class BaggageAsyncListenableTaskExecutor<E extends AsyncListenableTaskExecutor>
			extends BaggageAsyncTaskExecutor<E> implements AsyncListenableTaskExecutor {

		final static XTraceLogger xtrace = XTrace.getLogger(BaggageAsyncListenableTaskExecutor.class);

		public BaggageAsyncListenableTaskExecutor(E delegate) {
			super(delegate);
		}

		@Override
		public ListenableFuture<?> submitListenable(Runnable task) {
			xtrace.log("submitListenable runnable {}", task);
			BaggageRunnable baggageTask = BaggageRunnable.wrap(task);
			return BaggageListenableFuture.wrap(this.delegate.submitListenable(baggageTask), baggageTask);
		}

		@Override
		public <T> ListenableFuture<T> submitListenable(Callable<T> task) {
			xtrace.log("submitListenable callable {}", task);
			BaggageCallable<T> baggageTask = BaggageCallable.wrap(task);
			return BaggageListenableFuture.wrap(this.delegate.submitListenable(baggageTask), baggageTask);
		}
	}

	static class BaggageExecutorService<E extends ExecutorService> extends BaggageExecutor<E>
			implements ExecutorService {

		final static XTraceLogger xtrace = XTrace.getLogger(BaggageExecutorService.class);

		public BaggageExecutorService(E delegate) {
			super(delegate);
		}

		@Override
		public void shutdown() {
			this.delegate.shutdown();
		}

		@Override
		public List<Runnable> shutdownNow() {
			return this.delegate.shutdownNow();
		}

		@Override
		public boolean isShutdown() {
			return this.delegate.isShutdown();
		}

		@Override
		public boolean isTerminated() {
			return this.delegate.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return this.delegate.awaitTermination(timeout, unit);
		}

		@Override
		public <T> Future<T> submit(Callable<T> task) {
			xtrace.log("submitCallable {}", task);
			BaggageCallable<T> baggageTask = BaggageCallable.wrap(task);
			return BaggageFuture.wrap(this.delegate.submit(baggageTask), baggageTask);
		}

		@Override
		public <T> Future<T> submit(Runnable task, T result) {
			xtrace.log("submitRunnableWithDefault {}", task);
			BaggageRunnable baggageTask = BaggageRunnable.wrap(task);
			return BaggageFuture.wrap(this.delegate.submit(baggageTask, result), baggageTask);
		}

		@Override
		public Future<?> submit(Runnable task) {
			xtrace.log("submitRunnable {}", task);
			BaggageRunnable baggageTask = BaggageRunnable.wrap(task);
			return BaggageFuture.wrap(this.delegate.submit(baggageTask), baggageTask);
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
			xtrace.log("invokeAll {}", tasks);
			List<BaggageCallable<T>> baggageTasks = new ArrayList<>(tasks.size());
			for (Callable<T> task : tasks) {
				baggageTasks.add(BaggageCallable.wrap(task));
			}
			List<Future<T>> futures = this.delegate.invokeAll(baggageTasks);
			List<Future<T>> baggageFutures = new ArrayList<>(tasks.size());
			for (int i = 0; i < baggageTasks.size(); i++) {
				baggageFutures.add(BaggageFuture.wrap(futures.get(i), baggageTasks.get(i)));
			}
			return baggageFutures;
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
				throws InterruptedException {
			xtrace.log("invokeAll {}", tasks);
			List<BaggageCallable<T>> baggageTasks = new ArrayList<>(tasks.size());
			for (Callable<T> task : tasks) {
				baggageTasks.add(BaggageCallable.wrap(task));
			}
			List<Future<T>> futures = this.delegate.invokeAll(baggageTasks, timeout, unit);
			List<Future<T>> baggageFutures = new ArrayList<>(tasks.size());
			for (int i = 0; i < baggageTasks.size(); i++) {
				baggageFutures.add(BaggageFuture.wrap(futures.get(i), baggageTasks.get(i)));
			}
			return baggageFutures;
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			xtrace.log("invokeAny {}", tasks);
			List<BaggageCallableWithWrappedResult<T>> baggageTasks = new ArrayList<>(tasks.size());
			for (Callable<T> task : tasks) {
				baggageTasks.add(BaggageCallableWithWrappedResult.wrap(task));
			}
			WithBaggage<T> wrappedResult = this.delegate.invokeAny(baggageTasks);
			Baggage.join(wrappedResult.baggage);
			return wrappedResult.result;
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			xtrace.log("invokeAny {}", tasks);
			List<BaggageCallableWithWrappedResult<T>> baggageTasks = new ArrayList<>(tasks.size());
			for (Callable<T> task : tasks) {
				baggageTasks.add(BaggageCallableWithWrappedResult.wrap(task));
			}
			WithBaggage<T> wrappedResult = this.delegate.invokeAny(baggageTasks, timeout, unit);
			Baggage.join(wrappedResult.baggage);
			return wrappedResult.result;
		}

	}

}
