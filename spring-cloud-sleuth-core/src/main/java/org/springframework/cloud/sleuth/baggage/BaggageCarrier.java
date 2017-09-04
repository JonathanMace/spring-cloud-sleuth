package org.springframework.cloud.sleuth.baggage;

import brown.tracingplane.BaggageContext;

public interface BaggageCarrier {
	
	public BaggageContext getBaggageContext();

}
