/*
 * Copyright (C) 2019 Ryan Murray
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
