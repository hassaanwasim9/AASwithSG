package org.eclipse.basyx.components.aas.aascomponent;

import java.util.List;

import org.eclipse.basyx.aas.aggregator.api.IAASAggregatorFactory;
import org.eclipse.basyx.aas.registration.api.IAASRegistry;
import org.eclipse.basyx.aas.restapi.api.IAASAPIFactory;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.s3.S3AASAPIFactory;
import org.eclipse.basyx.components.aas.s3.S3AASAggregatorFactory;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAPIFactory;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAggregatorFactory;
import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregatorFactory;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPIFactory;

import com.amazonaws.services.s3.AmazonS3;

public class S3AASServerComponentFactory extends AbstractAASServerComponentFactory {
	private BaSyxS3Configuration config;
	private AmazonS3 s3Client;
	private IAASRegistry registry;
	
	public S3AASServerComponentFactory(BaSyxS3Configuration config, List<IAASServerDecorator> decorators, IAASRegistry registry) {
		this.config = config;
		this.aasServerDecorators = decorators;
		this.s3Client = S3Helper.createS3Client(config);
		this.registry = registry;
	}
	

	@Override
	protected ISubmodelAPIFactory createSubmodelAPIFactory() {
		return new S3SubmodelAPIFactory(s3Client,config.getSubmodelBucketName());
	}

	@Override
	protected ISubmodelAggregatorFactory createSubmodelAggregatorFactory(ISubmodelAPIFactory submodelAPIFactory) {
		return new S3SubmodelAggregatorFactory(s3Client, config.getSubmodelBucketName(), submodelAPIFactory);
	}

	@Override
	protected IAASAPIFactory createAASAPIFactory() {
		return new S3AASAPIFactory(config, s3Client);
	}

	@Override
	protected IAASAggregatorFactory createAASAggregatorFactory(IAASAPIFactory aasAPIFactory,
			ISubmodelAggregatorFactory submodelAggregatorFactory) {
		return new S3AASAggregatorFactory(registry, config, aasAPIFactory, submodelAggregatorFactory, s3Client);
	}

}
