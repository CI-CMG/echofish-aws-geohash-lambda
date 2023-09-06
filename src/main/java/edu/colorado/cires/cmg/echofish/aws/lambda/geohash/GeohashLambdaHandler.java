package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.S3ClientWrapper;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeohashLambdaHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeohashLambdaHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();

  private final S3ClientWrapper s3;
  private final GeohashLambdaConfiguration configuration;

  public GeohashLambdaHandler(S3ClientWrapper s3, GeohashLambdaConfiguration configuration) {
    this.s3 = s3;
    this.configuration = configuration;
  }

  public Void handleRequest(CruiseProcessingMessage cruiseProcessingMessage) {

    LOGGER.info("Started Event: {}", cruiseProcessingMessage);

    GeohashEventContext eventContext = new GeohashEventContext(
        configuration.getZarrBucketName(),
        cruiseProcessingMessage.getShipName(),
        cruiseProcessingMessage.getCruiseName(),
        cruiseProcessingMessage.getSensorName(),
        configuration.getMaxUploadBuffers());

    LOGGER.info("Context: {}", eventContext);

    GeoHashProcessor.process(s3, eventContext, OBJECT_MAPPER);

    LOGGER.info("Finished Event: {}", cruiseProcessingMessage);

    return null;
  }
}
