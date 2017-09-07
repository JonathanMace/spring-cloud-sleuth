package org.springframework.cloud.sleuth.baggage;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import edu.brown.cs.systems.baggage.Baggage;
import edu.brown.cs.systems.baggage.DetachedBaggage;

public class BaggageHttpServletResponse extends HttpServletResponseWrapper {

	public BaggageHttpServletResponse(HttpServletResponse response) {
		super(response);
	}

	@Override
	public void setStatus(int sc) {
		super.setStatus(sc);
		addBaggage();
	}

	@Override
	@SuppressWarnings("deprecation")
	public void setStatus(int sc, String sm) {
		super.setStatus(sc, sm);
		addBaggage();
	}

	@Override
	public void sendError(int sc, String msg) throws IOException {
		super.sendError(sc, msg);
		addBaggage();
	}

	@Override
	public void sendError(int sc) throws IOException {
		super.sendError(sc);
		addBaggage();
	}

	private void addBaggage() {
		DetachedBaggage forked = Baggage.fork();
		if (forked == null) {
			return;
		}
		String baggageString = forked.toStringBase64();
		if (baggageString == null) {
			return;
		}
		super.addHeader("Baggage", baggageString);
	}

}
