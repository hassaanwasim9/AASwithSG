package org.eclipse.basyx.regression.AASServer.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAPI;
import org.eclipse.basyx.regression.AASServer.SimpleNoOpAASSubmodel;
import org.eclipse.basyx.regression.AASServer.mongodb.TestMongoDBSubmodelProvider;
import org.eclipse.basyx.submodel.restapi.SubmodelProvider;
import org.eclipse.basyx.testsuite.regression.vab.protocol.http.TestsuiteDirectory;
import org.eclipse.basyx.vab.manager.VABConnectionManager;
import org.eclipse.basyx.vab.modelprovider.api.IModelProvider;
import org.eclipse.basyx.vab.protocol.api.ConnectorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.amazonaws.services.s3.AmazonS3;

/**
 * Test the submodel API with s3 backend
 * @author zhangzai
 *
 */
public class TestS3SubmodelProvider extends TestMongoDBSubmodelProvider {
	private static final String BUCKET_NAME = "sm-bucket-001";
	private static AmazonS3 s3Client;
	private VABConnectionManager connManager;
	
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
		
		S3Helper.createBucketIfNotExists(s3Client, BUCKET_NAME);
	}
	
	@AfterClass
	public static void tearDown() {
		S3Helper.deleteBucketIfExists(s3Client, BUCKET_NAME);
	}
	
	@Override
	protected VABConnectionManager getConnectionManager() {
		if (connManager == null) {
			connManager = new VABConnectionManager(new TestsuiteDirectory(), new ConnectorFactory() {
				@Override
				protected IModelProvider createProvider(String addr) {
					SimpleNoOpAASSubmodel submodel = new SimpleNoOpAASSubmodel();
					S3SubmodelAPI api = new S3SubmodelAPI(s3Client, BUCKET_NAME, "mySubmodelId");
					IModelProvider smProvider = null;
					try {
						api.setSubmodel(submodel);
						smProvider = new SubmodelProvider(api);
					} catch (IOException e) {
						e.printStackTrace();
					}
					return smProvider;
				}
			});
		}
		return connManager;
	}

	
}
