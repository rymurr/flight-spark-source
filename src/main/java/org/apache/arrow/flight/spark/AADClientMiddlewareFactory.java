package org.apache.arrow.flight.spark;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.FlightClientMiddleware;

public class AADClientMiddlewareFactory implements FlightClientMiddlewareFactory {
    private final String clientId;
    private final String clientSecret;
    private final String scope;

    public AADClientMiddlewareFactory(String clientId, String clientSecret, String scope) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
    }

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
        TokenCredential crediential = new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).build();
        TokenRequestContext context = new TokenRequestContext();
        context.addScopes(scope);
        return new AADClientMiddleware(crediential, context);
    }

}
