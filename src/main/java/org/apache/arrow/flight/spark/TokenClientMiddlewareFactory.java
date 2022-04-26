package org.apache.arrow.flight.spark;

import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightClientMiddleware;

public class TokenClientMiddlewareFactory implements FlightClientMiddlewareFactory {
    private final String token;

    public TokenClientMiddlewareFactory(String token) {
        this.token = token;
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
        return new TokenClientMiddleware(token);
    }

}
