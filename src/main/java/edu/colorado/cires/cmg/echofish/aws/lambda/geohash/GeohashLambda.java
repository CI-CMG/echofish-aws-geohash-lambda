package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import edu.colorado.cires.cmg.awszarr.AwsS3ClientWrapper;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public class GeohashLambda implements RequestHandler<SNSEvent, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeohashLambda.class);

  private static final GeohashLambdaHandler HANDLER = new GeohashLambdaHandler(
      AwsS3ClientWrapper.builder().s3(S3Client.builder().build()).build(),
      new GeohashLambdaConfiguration(
          Objects.requireNonNull(System.getenv("ZARR_BUCKET_NAME")),
          Integer.parseInt(System.getenv("S3_UPLOAD_BUFFERS")),
          geohashS3BucketName));

  @Override
  public Void handleRequest(SNSEvent snsEvent, Context context) {

    LOGGER.info("Received event: {}", snsEvent);

    SnsMessage snsMessage;
    try {
      snsMessage = TheObjectMapper.OBJECT_MAPPER.readValue(snsEvent.getRecords().get(0).getSNS().getMessage(), SnsMessage.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse SNS notification", e);
    }

    HANDLER.handleRequest(snsMessage);

    return null;
  }
}
