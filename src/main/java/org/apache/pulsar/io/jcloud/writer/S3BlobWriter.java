package org.apache.pulsar.io.jcloud.writer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.jcloud.sink.CloudStorageSinkConfig;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.PublicAccessType;

/**
 * An implementation of BlobWriter that uses the native Azure SDK, which offers a more direct API
 * and better performance.
 */
@Slf4j
public class S3BlobWriter implements BlobWriter {

    private final BlobServiceClient blobServiceClient;
    private final String container;
    private PublicAccessType accessType;

    public S3BlobWriter(CloudStorageSinkConfig sinkConfig) {

        blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(sinkConfig.getEndpoint())
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        container = sinkConfig.getBucket();
        if (StringUtils.isNotEmpty(sinkConfig.getAwsCannedAcl())) {
            accessType = PublicAccessType.fromString(sinkConfig.getAwsCannedAcl());
        }
    }

    @Override
    public void uploadBlob(String key, ByteBuffer payload) throws IOException {
        BlobClient blobClient = blobServiceClient.getBlobContainerClient(container).getBlobClient(key);
        BlobHttpHeaders headers = new BlobHttpHeaders();
        if (accessType != null) {
            headers.setBlobPublicAccess(accessType);
        }

        blobClient.upload(payload, payload.remaining(), true);
        blobClient.setHttpHeaders(headers);
    }

    @Override
    public void close() throws IOException {
        // No need to close BlobServiceClient as it doesn't hold any open resources
    }
}