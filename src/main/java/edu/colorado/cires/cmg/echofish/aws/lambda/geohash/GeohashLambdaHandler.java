package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeohashLambdaHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeohashLambdaHandler.class);

  private final S3ClientWrapper s3;
  private final GeohashLambdaConfiguration configuration;

  public GeohashLambdaHandler(S3ClientWrapper s3, GeohashLambdaConfiguration configuration) {
    this.s3 = s3;
    this.configuration = configuration;
  }

  public Void handleRequest(SnsMessage snsMessage) {

    LOGGER.info("Started Event: {}", snsMessage);

    GeohashEventContext eventContext = new GeohashEventContext(
        configuration.getZarrBucketName(),
        snsMessage.getSurvey(),
        configuration.getMaxUploadBuffers(),
        configuration.getGeohashS3BucketName());

    LOGGER.info("Context: {}", eventContext);

    GeoHashProcessor.process(s3, eventContext, TheObjectMapper.OBJECT_MAPPER);

    LOGGER.info("Finished Event: {}", snsMessage);

    return null;
  }
}
