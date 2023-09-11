package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.geo.GeoHash;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoHashProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoHashProcessor.class);
  private static final int LENGTH = 5;

  private static void saveIndexFile(S3ClientWrapper s3, GeohashEventContext event, ObjectMapper objectMapper, IndexFile indexFile) {
    try (S3OutputStream out = S3OutputStream.builder()
        .s3(s3)
        .bucket(event.getS3BucketName())
        .key(indexFile.getPath())
        .autoComplete(false)
        .uploadQueueSize(event.getMaxUploadBuffers())
        .build()
    ) {
      LOGGER.info("Writing {}/{}", event.getS3BucketName(), indexFile.getPath());
      objectMapper.writeValue(out, indexFile);
      out.done();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to write geohash", e);
    }
  }

  private static IndexFile readOrNewIndexFile(S3ClientWrapper s3, GeohashEventContext event, ObjectMapper objectMapper, String path) {
    Optional<InputStream> maybeNext = s3.getObject(event.getS3BucketName(), path);
    if (maybeNext.isPresent()) {
      try (InputStream in = maybeNext.get()) {
        return objectMapper.readValue(in, IndexFile.class);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to read geohash", e);
      }
    }
    return new IndexFile();
  }

  public static boolean isValid(DataPoint row) {
    return row.getTime() != 0L
        && !Double.isNaN(row.getLatitude())
        && !Double.isNaN(row.getLongitude())
        && Math.abs(row.getLatitude() - 0D) > 0.00001
        && Math.abs(row.getLongitude() - 0D) > 0.00001;
  }

  public static void process(S3ClientWrapper s3, GeohashEventContext event, ObjectMapper objectMapper) {
    DataPointZarrIterator iterator;
    String zarrKey =
        "level_2/" + event.getShipName() + "/" + event.getCruiseName() + "/" + event.getSensorName() + "/" + event.getCruiseName() + ".zarr";
    try {
      iterator = new DataPointZarrIterator(s3, event.getS3BucketName(), zarrKey);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read zarr store", e);
    }
    int i = -1;
    IndexFile indexFile = null;
    while (iterator.hasNext()) {

      DataPoint row = iterator.next();
      i++;
      if (isValid(row)) {
        String hash = GeoHash.encodeHash(row.getLatitude(), row.getLongitude(), LENGTH);
        String path =
            "spatial/geohash/cruise/" + event.getShipName() + "/" + event.getCruiseName() + "/" + event.getSensorName() + "/" + hash + ".json";

        if (indexFile == null) {
          indexFile = new IndexFile();
          indexFile.setPath(path);
        }

        IndexFile.Record record = new IndexFile.Record();
        record.setIndex(i);
        record.setLongitude(row.getLongitude());
        record.setLatitude(row.getLatitude());

        if (!path.equals(indexFile.getPath())) {
          saveIndexFile(s3, event, objectMapper, indexFile);
          indexFile = readOrNewIndexFile(s3, event, objectMapper, path);
          indexFile.setPath(path);
        }

        indexFile.getIndexRecords().add(record);
      } else {
        LOGGER.warn("Invalid point: {} : {}", i, row);
      }
    }

    if (indexFile != null) {
      saveIndexFile(s3, event, objectMapper, indexFile);
    }
  }


}
