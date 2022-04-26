package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

public class TokenClientMiddleware implements FlightClientMiddleware {
    private final String token;

    public TokenClientMiddleware(String token) {
        this.token = token;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert("authorization", String.format("Bearer %s", token));
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        // Nothing needed here
    }

    @Override
    public void onCallCompleted(CallStatus status) {
        // Nothing needed here
    }
    
}
