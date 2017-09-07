package org.springframework.cloud.sleuth.instrument.async;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.cloud.sleuth.DefaultSpanNamer;
import org.springframework.cloud.sleuth.SpanNamer;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;

public class TraceExecutors {

	public static Executor wrapAsLocalComponent(BeanFactory beanFactory, Executor delegate) {
		return wrap(delegate, new LocalComponentWrapFunction(beanFactory));
	}

	public static Executor wrapWithTracer(Tracer tracer, Executor delegate) {
		return wrap(delegate, new TracerWrapFunction(tracer));
	}

	private static Executor wrap(Executor delegate, WrapFunction wrapFunction) {
		if (delegate instanceof AsyncListenableTaskExecutor) {
			return new TraceAsyncListenableTaskExecutorBase<AsyncListenableTaskExecutor>(
					(AsyncListenableTaskExecutor) delegate, wrapFunction);
		} else if (delegate instanceof AsyncTaskExecutor) {
			return new TraceAsyncTaskExecutorBase<AsyncTaskExecutor>((AsyncTaskExecutor) delegate, wrapFunction);
		} else {
			return new TraceExecutorBase<Executor>(delegate, wrapFunction);
		}
	}

	private static interface WrapFunction {
		Runnable wrap(Runnable runnable);

		<V> Callable<V> wrap(Callable<V> callable);
	}

	private static class TracerWrapFunction implements WrapFunction {

		private final Tracer tracer;

		public TracerWrapFunction(Tracer tracer) {
			this.tracer = tracer;
		}

		@Override
		public Runnable wrap(Runnable runnable) {
			return tracer.wrap(runnable);
		}

		@Override
		public <V> Callable<V> wrap(Callable<V> callable) {
			return tracer.wrap(callable);
		}

	}

	private static class LocalComponentWrapFunction implements WrapFunction {

		protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

		protected Tracer tracer;
		protected final BeanFactory beanFactory;
		protected TraceKeys traceKeys;
		protected SpanNamer spanNamer;

		public LocalComponentWrapFunction(BeanFactory beanFactory) {
			this.beanFactory = beanFactory;
		}

		// due to some race conditions trace keys might not be ready yet
		private TraceKeys traceKeys() {
			if (this.traceKeys == null) {
				try {
					this.traceKeys = this.beanFactory.getBean(TraceKeys.class);
				} catch (NoSuchBeanDefinitionException e) {
					log.warn("TraceKeys bean not found - will provide a manually created instance");
					return new TraceKeys();
				}
			}
			return this.traceKeys;
		}

		// due to some race conditions trace keys might not be ready yet
		private SpanNamer spanNamer() {
			if (this.spanNamer == null) {
				try {
					this.spanNamer = this.beanFactory.getBean(SpanNamer.class);
				} catch (NoSuchBeanDefinitionException e) {
					log.warn("SpanNamer bean not found - will provide a manually created instance");
					return new DefaultSpanNamer();
				}
			}
			return this.spanNamer;
		}

		private Tracer getTracer() {
			if (this.tracer == null) {
				try {
					this.tracer = this.beanFactory.getBean(Tracer.class);
				} catch (NoSuchBeanDefinitionException e) {
				}
			}
			return this.tracer;
		}

		@Override
		public Runnable wrap(Runnable command) {
			if (getTracer() != null && !(command instanceof LocalComponentTraceRunnable)) {
				return new LocalComponentTraceRunnable(this.tracer, traceKeys(), spanNamer(), command);
			}
			return command;
		}

		@Override
		public <V> Callable<V> wrap(Callable<V> command) {
			if (getTracer() != null && !(command instanceof LocalComponentTraceCallable)) {
				return new LocalComponentTraceCallable<V>(this.tracer, traceKeys(), spanNamer(), command);
			}
			return command;
		}
	}

	public static class TraceExecutor extends TraceExecutorBase<Executor> {
		public TraceExecutor(BeanFactory beanFactory, Executor delegate) {
			super(delegate, new LocalComponentWrapFunction(beanFactory));
		}
	}

	/**
	 * {@link Executor} that wraps {@link Runnable} in a
	 * {@link org.springframework.cloud.sleuth.TraceRunnable TraceRunnable} that
	 * sets a local component tag on the span.
	 *
	 * @author Dave Syer
	 * @since 1.0.0
	 */
	static class TraceExecutorBase<E extends Executor> implements Executor {

		protected final E delegate;
		protected final WrapFunction wrapFunction;

		public TraceExecutorBase(E delegate, WrapFunction wrapFunction) {
			this.delegate = delegate;
			this.wrapFunction = wrapFunction;
		}

		@Override
		public void execute(Runnable command) {
			this.delegate.execute(wrapFunction.wrap(command));
		}
	}

	static class TraceAsyncTaskExecutorBase<E extends AsyncTaskExecutor> extends TraceExecutorBase<E>
			implements AsyncTaskExecutor {

		public TraceAsyncTaskExecutorBase(E delegate, WrapFunction wrapFunction) {
			super(delegate, wrapFunction);
		}

		@Override
		public void execute(Runnable task, long startTimeout) {
			this.delegate.execute(wrapFunction.wrap(task), startTimeout);
		}

		@Override
		public Future<?> submit(Runnable task) {
			return this.delegate.submit(wrapFunction.wrap(task));
		}

		@Override
		public <T> Future<T> submit(Callable<T> task) {
			return this.delegate.submit(wrapFunction.wrap(task));
		}

	}

	public static class TraceAsyncListenableTaskExecutor
			extends TraceAsyncListenableTaskExecutorBase<AsyncListenableTaskExecutor> {
		public TraceAsyncListenableTaskExecutor(AsyncListenableTaskExecutor delegate, Tracer tracer) {
			super(delegate, new TracerWrapFunction(tracer));
		}
	}

	static class TraceAsyncListenableTaskExecutorBase<E extends AsyncListenableTaskExecutor>
			extends TraceAsyncTaskExecutorBase<E> implements AsyncListenableTaskExecutor {

		public TraceAsyncListenableTaskExecutorBase(E delegate, WrapFunction wrapFunction) {
			super(delegate, wrapFunction);
		}

		@Override
		public ListenableFuture<?> submitListenable(Runnable task) {
			return this.delegate.submitListenable(wrapFunction.wrap(task));
		}

		@Override
		public <T> ListenableFuture<T> submitListenable(Callable<T> task) {
			return this.delegate.submitListenable(wrapFunction.wrap(task));
		}

	}

}
