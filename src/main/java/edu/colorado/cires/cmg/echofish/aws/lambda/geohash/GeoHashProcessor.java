package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoHashProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoHashProcessor.class);
  private static final int BITS = 25;


  public static void process(S3ClientWrapper s3, GeohashEventContext event, ObjectMapper objectMapper) {
    final String cruise = event.getSurvey();
//    Path targetBucketPath = s3fs.getPath("/" + targetBucket);
    DataPointZarrIterator iterator = null;
    try {
      iterator = new DataPointZarrIterator(s3, event.getS3BucketName(), cruise + ".zarr");
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read zarr store", e);
    }
    int i = 0;
    String currentPath = null;
    List<List<Number>> contents = new ArrayList<>();
    while (iterator.hasNext()) {
      DataPoint row = iterator.next();
      String hash = calculateGeohashString(row.getLongitude(), row.getLatitude(), BITS);
      String fileName = hash + ".json";
      String path = cruise + "/" + fileName;
      if (!path.equals(currentPath)) {

        //write current hash data
        if (!contents.isEmpty()) {
          try (S3OutputStream out = S3OutputStream.builder()
              .s3(s3)
              .bucket(event.getGeohashS3BucketName())
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
        Optional<InputStream> maybeNext = s3.getObject(event.getGeohashS3BucketName(), path);
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
          .bucket(event.getGeohashS3BucketName())
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

  private static final char[] BASE_32 = new char[]{
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
      'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
      's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
  };


  private static int[] splitToDecimalArray(long hash, int bits) {
    LinkedList<Integer> result = new LinkedList<>();
    long val = hash;
    int c = (int) Math.ceil((double) bits / 5d);
    for (int i = 0; i < c; i++) {
      result.push((int) (val & 31));
      val >>>= 5;
    }
    int[] array = new int[result.size()];
    Iterator<Integer> it = result.iterator();
    int i = 0;
    while (it.hasNext()) {
      array[i++] = it.next();
    }
    return array;
  }

  ;

  private static String decimalToGeohashBase32(long hash, int bits) {
    int[] parts = splitToDecimalArray(hash, bits);
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      result.append(BASE_32[parts[i]]);
    }
    return result.toString();
  }

  ;

  private static long calculateGeohash(double lon, double lat, int bits) {
    double minLat = -90;
    double maxLat = 90;
    double minLon = -180;
    double maxLon = 180;
    long result = 0;
    for (int i = 0; i < bits; i++) {
      if (i % 2 == 0) { // even bit: bisect longitude
        double midpoint = (minLon + maxLon) / 2d;
        if (lon < midpoint) {
          result <<= 1; // push a zero bit
          maxLon = midpoint; // shrink range downwards
        } else {
          result = result << 1 | 1; // push a one bit
          minLon = midpoint; // shrink range upwards
        }
      } else { // odd bit: bisect latitude
        double midpoint = (minLat + maxLat) / 2d;
        if (lat < midpoint) {
          result <<= 1; // push a zero bit
          maxLat = midpoint; // shrink range downwards
        } else {
          result = result << 1 | 1; // push a one bit
          minLat = midpoint; // shrink range upwards
        }
      }
    }
    return result;
  }

  private static String calculateGeohashString(double lon, double lat, int bits) {
    return decimalToGeohashBase32(calculateGeohash(lon, lat, bits), bits);
  }

}
