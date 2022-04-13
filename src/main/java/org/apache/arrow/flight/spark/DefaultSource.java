package org.apache.arrow.flight.spark;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.flight.Location;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class DefaultSource implements TableProvider, DataSourceRegister {

  private FlightTable makeTable(CaseInsensitiveStringMap options) {
    String protocol = options.getOrDefault("protocol", "grpc");
    Location location;
    if (protocol == "grpc+tls") {
      location = Location.forGrpcTls(
        options.getOrDefault("host", "localhost"),
        Integer.parseInt(options.getOrDefault("port", "47470"))
      );
    } else {
      location = Location.forGrpcInsecure(
        options.getOrDefault("host", "localhost"),
        Integer.parseInt(options.getOrDefault("port", "47470"))
      );
    }

    String sql = options.getOrDefault("path", "");
    String trustedCertificates = options.getOrDefault("trustedCertificates", "");
    Optional<InputStream> trustedCertificatesIs = trustedCertificates.isBlank() ? Optional.empty() : Optional.of(new ByteArrayInputStream(trustedCertificates.getBytes()));

    return new FlightTable(
      String.format("{} Location {} Command {}", shortName(), location.getUri().toString(), sql),
      location,
      sql,
      trustedCertificatesIs
    );
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return makeTable(options).schema();
  }

  @Override
  public String shortName() {
    return "flight";
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    return makeTable(new CaseInsensitiveStringMap(options));
  }
}
