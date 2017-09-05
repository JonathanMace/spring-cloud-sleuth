package org.springframework.cloud.sleuth.baggage;

import edu.brown.cs.systems.baggage.DetachedBaggage;

public interface BaggageCarrier {
	
	public DetachedBaggage getBaggageContext();

}
