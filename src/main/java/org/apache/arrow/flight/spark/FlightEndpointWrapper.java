package org.apache.arrow.flight.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;

// This is needed for FlightEndpoint to be Serializable in spark.
// org.apache.arrow.flight.FlightEndpoint is a POJO of Serializable types.
// However if spark is using build-in serialization instead of Kyro then we must implement Serializable
public class FlightEndpointWrapper implements Serializable {
    private FlightEndpoint inner;

    public FlightEndpointWrapper(FlightEndpoint inner) {
        this.inner = inner;
    }

    public FlightEndpoint get() {
        return inner;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        ArrayList<URI> locations = inner.getLocations().stream().map(location -> location.getUri()).collect(Collectors.toCollection(ArrayList::new));
        out.writeObject(locations);
        out.write(inner.getTicket().getBytes());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        @SuppressWarnings("unchecked")
        Location[] locations = ((ArrayList<URI>) in.readObject()).stream().map(l -> new Location(l)).toArray(Location[]::new);
        byte[] ticket = in.readAllBytes();
        this.inner = new FlightEndpoint(new Ticket(ticket), locations);
    }
}
