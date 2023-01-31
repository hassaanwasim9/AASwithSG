package org.eclipse.basyx.components.aas.s3;

import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.restapi.api.IAASAPI;
import org.eclipse.basyx.aas.restapi.api.IAASAPIFactory;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;

import com.amazonaws.services.s3.AmazonS3;

public class S3AASAPIFactory implements IAASAPIFactory {
	private BaSyxS3Configuration config;
	private AmazonS3 s3Client;
	
	public S3AASAPIFactory(BaSyxS3Configuration config, AmazonS3 s3Client) {
		this.config = config;
		this.s3Client = s3Client;
	}

	@Override
	public IAASAPI getAASApi(AssetAdministrationShell aas) {
		S3AASAPI s3AASAPI = new S3AASAPI(s3Client, config.getAASBucketName(), aas.getIdentification().getId());
		s3AASAPI.setAAS(aas);
		return s3AASAPI;
	}

}
