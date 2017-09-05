package org.springframework.cloud.sleuth.baggage;

import edu.brown.cs.systems.baggage.DetachedBaggage;

public class WithBaggage<T> {

	final T result;
	final DetachedBaggage baggage;

	public WithBaggage(T result, DetachedBaggage baggage) {
		this.result = result;
		this.baggage = baggage;
	}

}
