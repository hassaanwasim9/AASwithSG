package org.eclipse.basyx.regression.AASServer.s3;

import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.basyx.aas.metamodel.map.descriptor.CustomId;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAPIFactory;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAggregator;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregator;
import org.eclipse.basyx.submodel.metamodel.map.Submodel;
import org.eclipse.basyx.testsuite.regression.submodel.aggregator.SubmodelAggregatorSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;

public class TestS3SubmodelAggregator extends SubmodelAggregatorSuite {
	private static final String BUCKET_NAME = "sm-bucket-001";
	private static S3SubmodelAggregator smAggregator;
	private static AmazonS3 s3Client;
	
	@BeforeClass
	public static void init() {
		Map<String, String> s3ConfigurationValues = new HashMap<>();
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_HOST, "172.28.5.50");
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_PORT, "10443");
		s3ConfigurationValues.put(BaSyxS3Configuration.SIGNING_REGION, "us-east-1");
		s3ConfigurationValues.put(BaSyxS3Configuration.ACCESS_KEY, "WZ0TF9YWGWE8VSAJ4E4S");
		s3ConfigurationValues.put(BaSyxS3Configuration.SECRET_KEY, "NyG/EJ31ckuRsE9UwMT69/DCpf+4jJMAzfTboMEJ");


		BaSyxS3Configuration s3Config = new BaSyxS3Configuration(s3ConfigurationValues);
		s3Config.setDiableCertChecking(true);
		
		s3Client = S3Helper.createS3Client(s3Config);
		s3Config.setSubmodelBucketName(BUCKET_NAME);
		smAggregator = new S3SubmodelAggregator(new S3SubmodelAPIFactory(s3Client, BUCKET_NAME), s3Client, BUCKET_NAME);
		
		
		S3Helper.createBucketIfNotExists(s3Client, BUCKET_NAME);
	}
	
	@AfterClass
	public static void tearDown() {
		S3Helper.deleteBucketIfExists(s3Client, BUCKET_NAME);
	}

	@Test
	public void deletedSubmodelIsRemovedFromDB() {
		Submodel toDelete = new Submodel("deleteMeIdShort", new CustomId("deleteMe"));

		getSubmodelAggregator().createSubmodel(toDelete);

		getSubmodelAggregator().deleteSubmodelByIdentifier(toDelete.getIdentification());

		assertSubmodelDoesNotExist(toDelete);
	}
	
	
	private void assertSubmodelDoesNotExist(Submodel toDelete) {
		assertFalse(S3Helper.listBucketContentKeys(s3Client, BUCKET_NAME).contains(toDelete.getIdentification().getId()));
		
	}

	@Override
	protected ISubmodelAggregator getSubmodelAggregator() {
		return smAggregator;
	}

}
