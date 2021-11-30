package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import edu.colorado.cires.cmg.awszarr.AwsS3ZarrStore;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;

public class DataPointZarrIterator implements Iterator<DataPoint> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPointZarrIterator.class);
  public static final int DEFAULT_BUFFER_SIZE = 43520;

  private final S3ClientWrapper s3;
  private final ZarrGroup zarrGroup;
  private final ZarrArray latitudeArray;
  private final ZarrArray longitudeArray;
  private final ZarrArray timeArray;
  private final int bufferSize;
  private int count;
  private int remaining;
  private List<DataPoint> points;

  public DataPointZarrIterator(S3ClientWrapper s3, String s3BucketName, String s3BucketZarrKey) throws IOException {
    this(s3, s3BucketName, s3BucketZarrKey, DEFAULT_BUFFER_SIZE);
  }

  public DataPointZarrIterator(S3ClientWrapper s3, String s3BucketName, String s3BucketZarrKey, int bufferSize) throws IOException {
    this.bufferSize = bufferSize;
    points = new ArrayList<>(bufferSize);
    this.s3 = s3;
    zarrGroup = ZarrGroup.open(AwsS3ZarrStore.builder()
        .s3(s3)
        .bucket(s3BucketName)
        .key(s3BucketZarrKey)
        .build());
    latitudeArray = Objects.requireNonNull(zarrGroup.openArray("latitude"), "Missing latitude data");
    longitudeArray = Objects.requireNonNull(zarrGroup.openArray("longitude"), "Missing longitude data");
    timeArray = Objects.requireNonNull(zarrGroup.openArray("time"), "Missing time data");
    count = latitudeArray.getShape()[0];
    remaining = count;
  }

  private void read() {
    if (remaining > 0) {
      try {
        int readSize = Math.min(bufferSize, remaining);
        int offset = count - remaining;
        double[] longitudeChunk = (double[]) longitudeArray.read(new int[]{readSize}, new int[]{offset});
        double[] latitudeChunk = (double[]) latitudeArray.read(new int[]{readSize}, new int[]{offset});
        double[] timeChunk = (double[]) timeArray.read(new int[]{readSize}, new int[]{offset});
        //TODO get latest version to allow concurrency
//        double[] longitudeChunk = (double[]) longitudeArray.readConcurrently(new int[]{readSize}, new int[]{offset}, ForkJoinPool.commonPool());
//        double[] latitudeChunk = (double[]) latitudeArray.readConcurrently(new int[]{readSize}, new int[]{offset}, ForkJoinPool.commonPool());
//        double[] timeChunk = (double[]) timeArray.readConcurrently(new int[]{readSize}, new int[]{offset}, ForkJoinPool.commonPool());
        points = new ArrayList<>(readSize);
        for (int i = 0; i < readSize; i++) {
          DataPoint dataPoint = new DataPoint(longitudeChunk[i], latitudeChunk[i], (long) (timeChunk[i] * 1000));
          points.add(dataPoint);
        }
        remaining = remaining - readSize;
      } catch (InvalidRangeException | IOException e) {
        throw new IllegalStateException("Unable to read data", e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return remaining > 0 || !points.isEmpty();
  }

  @Override
  public DataPoint next() {
    if (points.isEmpty() && remaining == 0) {
      throw new NoSuchElementException("No more data");
    }
    if (points.isEmpty()) {
      read();
    }
    return points.remove(points.size() - 1);
  }

}
