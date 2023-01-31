package org.eclipse.basyx.components.aas.s3;

import java.io.IOException;

import org.eclipse.basyx.aas.aggregator.api.IAASAggregator;
import org.eclipse.basyx.aas.aggregator.api.IAASAggregatorFactory;
import org.eclipse.basyx.aas.registration.api.IAASRegistry;
import org.eclipse.basyx.aas.restapi.api.IAASAPIFactory;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregatorFactory;

import com.amazonaws.services.s3.AmazonS3;

public class S3AASAggregatorFactory implements IAASAggregatorFactory {
	private BaSyxS3Configuration config;
	private IAASAPIFactory aasAPIFactory;
	private ISubmodelAggregatorFactory smAggregatorFactory;
	private AmazonS3 s3Client;
	private IAASRegistry registry;
	

	public S3AASAggregatorFactory(IAASRegistry registry, BaSyxS3Configuration config, IAASAPIFactory aasAPIFactory,
			ISubmodelAggregatorFactory smAggregatorFactory, AmazonS3 s3Client) {
		super();
		this.config = config;
		this.aasAPIFactory = aasAPIFactory;
		this.smAggregatorFactory = smAggregatorFactory;
		this.s3Client = s3Client;
		this.registry = registry;
	}

	@Override
	public IAASAggregator create() {
		try {
			return new S3AASAggregator(registry, config, aasAPIFactory, smAggregatorFactory, s3Client);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
