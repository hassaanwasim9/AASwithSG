package org.eclipse.basyx.regression.AASServer.s3;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.basyx.aas.aggregator.api.IAASAggregator;
import org.eclipse.basyx.aas.manager.ConnectedAssetAdministrationShellManager;
import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.metamodel.map.descriptor.ModelUrn;
import org.eclipse.basyx.aas.registration.api.IAASRegistry;
import org.eclipse.basyx.aas.registration.memory.InMemoryRegistry;
import org.eclipse.basyx.components.aas.AASServerComponent;
import org.eclipse.basyx.components.aas.configuration.AASServerBackend;
import org.eclipse.basyx.components.aas.configuration.BaSyxAASServerConfiguration;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.s3.S3AASAPIFactory;
import org.eclipse.basyx.components.aas.s3.S3AASAggregator;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAggregatorFactory;
import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.components.aas.s3.S3SubmodelAPIFactory;
import org.eclipse.basyx.components.configuration.BaSyxContextConfiguration;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregatorFactory;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IIdentifier;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IdentifierType;
import org.eclipse.basyx.submodel.metamodel.api.reference.IKey;
import org.eclipse.basyx.submodel.metamodel.api.reference.IReference;
import org.eclipse.basyx.submodel.metamodel.api.reference.enums.KeyElements;
import org.eclipse.basyx.submodel.metamodel.api.reference.enums.KeyType;
import org.eclipse.basyx.submodel.metamodel.map.Submodel;
import org.eclipse.basyx.submodel.metamodel.map.identifier.Identifier;
import org.eclipse.basyx.submodel.metamodel.map.reference.Key;
import org.eclipse.basyx.submodel.metamodel.map.reference.Reference;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPIFactory;
import org.eclipse.basyx.testsuite.regression.aas.aggregator.AASAggregatorSuite;
import org.eclipse.basyx.vab.coder.json.serialization.DefaultTypeFactory;
import org.eclipse.basyx.vab.coder.json.serialization.GSONTools;
import org.eclipse.basyx.vab.protocol.api.IConnectorFactory;
import org.eclipse.basyx.vab.protocol.http.connector.HTTPConnectorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class TestS3AASAggregator extends AASAggregatorSuite {
	protected static final String AAS_ID = "aas001";
	protected static final String AAS_IDSHORT = "aasIdshort";
	private static final Identifier SM_IDENTIFICATION = new Identifier(IdentifierType.CUSTOM, "submodel001");
	private static final String SM_IDSHORT = "sm001";
	private static final String AAS_BUCKET_NAME = "aas-bucket001";
	private static final String SUBMODEL_BUCKET_NAME = "submodel-bucket001";

	protected static final String AAS_ID_2 = "aas002";


	private static AASServerComponent component;
	private static BaSyxS3Configuration s3Config;
	private static BaSyxContextConfiguration contextConfig;
	private static BaSyxAASServerConfiguration aasConfig;
	private static IAASRegistry registry;

	protected static ConnectedAssetAdministrationShellManager manager;
	private static AmazonS3 s3Client;

	@BeforeClass
	public static void setUpClass() throws IOException {
		Map<String, String> s3ConfigurationValues = new HashMap<>();
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_HOST, "172.28.5.50");
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_PORT, "10443");
		s3ConfigurationValues.put(BaSyxS3Configuration.SIGNING_REGION, "us-east-1");
		s3ConfigurationValues.put(BaSyxS3Configuration.ACCESS_KEY, "WZ0TF9YWGWE8VSAJ4E4S");
		s3ConfigurationValues.put(BaSyxS3Configuration.SECRET_KEY, "NyG/EJ31ckuRsE9UwMT69/DCpf+4jJMAzfTboMEJ");

		contextConfig = new BaSyxContextConfiguration();
		contextConfig.loadFromResource(BaSyxContextConfiguration.DEFAULT_CONFIG_PATH);

		s3Config = new BaSyxS3Configuration(s3ConfigurationValues);
		s3Config.setDiableCertChecking(true);

		aasConfig = new BaSyxAASServerConfiguration(AASServerBackend.S3, "");
		s3Client = S3Helper.createS3Client(s3Config);
		
		deleteExistingBucketsAndObjects();
		S3Helper.createBucketIfNotExists(s3Client, AAS_BUCKET_NAME);
		S3Helper.createBucketIfNotExists(s3Client, SUBMODEL_BUCKET_NAME);
		
		s3Config.setAASBucketName(AAS_BUCKET_NAME);
		s3Config.setSubmodelBucketName(SUBMODEL_BUCKET_NAME);

		component = new AASServerComponent(contextConfig, aasConfig, s3Config);
		registry = new InMemoryRegistry();

		IConnectorFactory connectorFactory = new HTTPConnectorFactory();
		manager = new ConnectedAssetAdministrationShellManager(registry, connectorFactory);

		component.setRegistry(registry);
		component.startComponent();

		s3Client = S3Helper.createS3Client(s3Config);
		

	}

	private static void deleteExistingBucketsAndObjects() {
		List<Bucket> buckets = s3Client.listBuckets();
		for (Bucket buc : buckets) {
			if (buc.getName().contains("basyx_s3_bucket") || buc.getName().contains("([a-zA-Z]+)Bucket001")) {
				if (S3Helper.isVersionedBucket(s3Client, buc.getName())) {
					S3Helper.wipeVersionedBucket(s3Client, buc.getName());
				} else {
					S3Helper.wipeUnversionedBucket(s3Client, buc.getName());
				}
			}
		}
	}

	private static void createAssetAdministrationShell(String aasId) {
		AssetAdministrationShell assetAdministrationShell = new AssetAdministrationShell();
		IIdentifier identifier = new ModelUrn(aasId);

		assetAdministrationShell.setIdentification(identifier);
		assetAdministrationShell.setIdShort(AAS_IDSHORT);

		manager.createAAS(assetAdministrationShell, getURL());
	}


	@AfterClass
	public static void tearDown() {
		S3Helper.deleteBucketIfExists(s3Client, s3Config.getAASBucketName());
		S3Helper.deleteBucketIfExists(s3Client, s3Config.getSubmodelBucketName());
		component.stopComponent();
	}

	@Test
	public void testGetAASAndSubmodels() throws IOException {
		createAssetAdministrationShell(AAS_ID);
		createSubmodel(SM_IDSHORT, SM_IDENTIFICATION, AAS_ID);
		
		ObjectListing objects = s3Client.listObjects(AAS_BUCKET_NAME);
		for (S3ObjectSummary os : objects.getObjectSummaries()) {
			System.out.println("object key: " + os.getKey());
		}
		String aasJson = S3Helper.getBaSyxObjectContent(s3Client, AAS_BUCKET_NAME, AAS_ID);
		Object aasObject = new GSONTools(new DefaultTypeFactory()).deserialize(aasJson);
		@SuppressWarnings("unchecked")
		AssetAdministrationShell aas = AssetAdministrationShell.createAsFacade((Map<String, Object>) aasObject);
		assertEquals(AAS_ID, aas.getIdentification().getId());
		assertEquals(AAS_IDSHORT, aas.getIdShort());
		IReference submodelRef = aas.getSubmodelReferences().iterator().next();
		List<IKey> keys = submodelRef.getKeys();
		IKey lastKey = keys.get(keys.size() - 1);
		assertEquals(SM_IDENTIFICATION.getId(), lastKey.getValue());
		
		manager.deleteSubmodel(new ModelUrn(AAS_ID), SM_IDENTIFICATION);
		manager.deleteAAS(new ModelUrn(AAS_ID));
	}
	
	@Test
	public void testGetSubmodelWithIdshortReference() throws IOException {
		AssetAdministrationShell aas = new AssetAdministrationShell();
		aas.setIdentification(new ModelUrn(AAS_ID));
		aas.setIdShort(AAS_IDSHORT);
		Key k = new Key(KeyElements.SUBMODEL, false, SM_IDSHORT, KeyType.IDSHORT);
		Reference ref = new Reference(Collections.singletonList(k));
		aas.addSubmodelReference(ref);
		
		manager.createAAS(aas, getURL());
		
		//get the aas
		String aasJson = S3Helper.getBaSyxObjectContent(s3Client, AAS_BUCKET_NAME, AAS_ID);
		@SuppressWarnings("unchecked")
		AssetAdministrationShell fetched = AssetAdministrationShell.createAsFacade((Map<String, Object>)(new GSONTools(new DefaultTypeFactory()).deserialize(aasJson)));
		IReference submodelRef = fetched.getSubmodelReferences().iterator().next();
		List<IKey> keys = submodelRef.getKeys();
		IKey lastKey = keys.get(keys.size() - 1);
		assertEquals(SM_IDSHORT, lastKey.getValue());
		
		manager.deleteAAS(new ModelUrn(AAS_ID));
	} 

	protected static String getURL() {
		return component.getURL();
	}

	private static void createSubmodel(String smIdShort, Identifier smIdentifier, String aasId) {
		Submodel sm = new Submodel(smIdShort, smIdentifier);
		manager.createSubmodel(new ModelUrn(aasId), sm);
	}

	@Override
	protected IAASAggregator getAggregator() {
		ISubmodelAPIFactory submodelApiFactory = new S3SubmodelAPIFactory(s3Client, s3Config.getSubmodelBucketName());
		ISubmodelAggregatorFactory s3SubmodelAggregatorFactory = new S3SubmodelAggregatorFactory(s3Client,
				s3Config.getAASBucketName(), submodelApiFactory);
		S3AASAggregator s3AASAggregator = null;
		try {
			s3AASAggregator = new S3AASAggregator(registry, s3Config, new S3AASAPIFactory(s3Config, s3Client),
					s3SubmodelAggregatorFactory, s3Client);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return s3AASAggregator;
	}

}
