package org.apache.arrow.flight.spark;

import java.io.Serializable;

import org.apache.arrow.flight.FlightClientMiddleware;

public interface FlightClientMiddlewareFactory extends FlightClientMiddleware.Factory, Serializable {

}
