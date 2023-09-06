package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.geo.GeoHash;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoHashProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoHashProcessor.class);
  private static final int LENGTH = 5;


  public static void process(S3ClientWrapper s3, GeohashEventContext event, ObjectMapper objectMapper) {
    DataPointZarrIterator iterator;
    String zarrKey = "level_2/" + event.getShipName() + "/" + event.getCruiseName() + "/" + event.getSensorName() + "/" + event.getCruiseName() + ".zarr";
    try {
      iterator = new DataPointZarrIterator(s3, event.getS3BucketName(), zarrKey);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read zarr store", e);
    }
    int i = 0;
    String currentPath = null;
    List<List<Number>> contents = new ArrayList<>();
    while (iterator.hasNext()) {
      DataPoint row = iterator.next();
      String hash = GeoHash.encodeHash(row.getLatitude(), row.getLongitude(), LENGTH);
      String path = "spatial/geojson/cruise/" + event.getShipName() + "/" + event.getCruiseName() + "/" + event.getSensorName() + "/" + hash + ".json";
      if (!path.equals(currentPath)) {

        //write current hash data
        if (!contents.isEmpty()) {
          try (S3OutputStream out = S3OutputStream.builder()
              .s3(s3)
              .bucket(event.getS3BucketName())
              .key(path)
              .autoComplete(false)
              .uploadQueueSize(event.getMaxUploadBuffers())
              .build()
          ) {
            LOGGER.info("Writing {}", currentPath);
            objectMapper.writeValue(out, contents);
            out.done();
          } catch (IOException e) {
            throw new IllegalStateException("Unable to write geohash", e);
          }
        }

        //get next hash data
        Optional<InputStream> maybeNext = s3.getObject(event.getS3BucketName(), path);
        if (maybeNext.isPresent()) {
          try (InputStream in = maybeNext.get()) {
            contents = objectMapper.readValue(in, new TypeReference<>() {
            });
          } catch (IOException e) {
            throw new IllegalStateException("Unable to read geohash", e);
          }
        } else {
          contents = new ArrayList<>();
        }

        currentPath = path;
      }

      List<Number> values = new ArrayList<>();
      values.add(i);
      values.add(row.getLongitude());
      values.add(row.getLatitude());
      contents.add(values);

      i++;
    }

    if (!contents.isEmpty()) {
      try (S3OutputStream out = S3OutputStream.builder()
          .s3(s3)
          .bucket(event.getS3BucketName())
          .key(currentPath)
          .autoComplete(false)
          .uploadQueueSize(event.getMaxUploadBuffers())
          .build()) {
        LOGGER.info("Writing {}", currentPath);
        objectMapper.writeValue(out, contents);
        out.done();
      } catch (IOException e) {
        throw new IllegalStateException("Unable to write geohash", e);
      }
    }
  }


}
