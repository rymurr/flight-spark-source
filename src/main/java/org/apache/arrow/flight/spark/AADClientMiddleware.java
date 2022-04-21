package org.apache.arrow.flight.spark;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;

public class AADClientMiddleware implements FlightClientMiddleware {
    private final TokenCredential crediential;
    private final TokenRequestContext requestContext;

    public AADClientMiddleware(TokenCredential crediential, TokenRequestContext requestContext) {
        this.crediential = crediential;
        this.requestContext = requestContext;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert("authorization", String.format("Bearer %s", crediential.getToken(requestContext).block().getToken()));
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
