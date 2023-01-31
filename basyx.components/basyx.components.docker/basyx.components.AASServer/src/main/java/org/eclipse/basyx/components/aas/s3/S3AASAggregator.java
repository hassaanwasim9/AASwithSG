package org.eclipse.basyx.components.aas.s3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.basyx.aas.aggregator.api.IAASAggregator;
import org.eclipse.basyx.aas.metamodel.api.IAssetAdministrationShell;
import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.registration.api.IAASRegistry;
import org.eclipse.basyx.aas.restapi.AASModelProvider;
import org.eclipse.basyx.aas.restapi.MultiSubmodelProvider;
import org.eclipse.basyx.aas.restapi.api.IAASAPI;
import org.eclipse.basyx.aas.restapi.api.IAASAPIFactory;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregator;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregatorFactory;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IIdentifier;
import org.eclipse.basyx.submodel.metamodel.api.reference.IKey;
import org.eclipse.basyx.submodel.metamodel.api.reference.IReference;
import org.eclipse.basyx.submodel.metamodel.api.reference.enums.KeyType;
import org.eclipse.basyx.submodel.restapi.SubmodelProvider;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPI;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPIFactory;
import org.eclipse.basyx.vab.coder.json.serialization.DefaultTypeFactory;
import org.eclipse.basyx.vab.coder.json.serialization.GSONTools;
import org.eclipse.basyx.vab.exception.provider.ResourceNotFoundException;
import org.eclipse.basyx.vab.modelprovider.api.IModelProvider;
import org.eclipse.basyx.vab.protocol.api.IConnectorFactory;
import org.eclipse.basyx.vab.protocol.http.connector.HTTPConnectorFactory;
import org.slf4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

/**
 * AAS Aggregator with S3 backend
 * 
 * @author zhangzai
 *
 */
public class S3AASAggregator implements IAASAggregator {
	private static final String IDENTIFIER_TAG = "identifier";
	private static final String IDSHORT_TAG = "idShort";
	private AmazonS3 s3Client;
	private String aasBucketName;
	private String submodelBucketName;
	protected IAASAPIFactory aasApiProvider;
	protected ISubmodelAPIFactory smApiProvider;
	protected ISubmodelAggregatorFactory submodelAggregatorFactory;
	protected Map<String, MultiSubmodelProvider> aasProviderMap = new HashMap<>();
	private IAASRegistry registry;

	private GSONTools gsonTools = new GSONTools(new DefaultTypeFactory());
	private static Logger logger = org.slf4j.LoggerFactory.getLogger(S3AASAggregator.class);

	public S3AASAggregator(IAASRegistry registry, BaSyxS3Configuration config, IAASAPIFactory aasAPIFactory,
			ISubmodelAggregatorFactory submodelAggregatorFactory, AmazonS3 client) throws IOException {
		setS3Configuration(config);
		this.aasApiProvider = aasAPIFactory;
		this.submodelAggregatorFactory = submodelAggregatorFactory;
		this.registry = registry;
		init();
	}

	private void setS3Configuration(BaSyxS3Configuration config) {
		this.aasBucketName = config.getAASBucketName();
		this.submodelBucketName = config.getSubmodelBucketName();
		this.s3Client = S3Helper.createS3Client(config);
	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	private void init() throws IOException {
		ListObjectsV2Result result = s3Client.listObjectsV2(aasBucketName);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary os : objects) {
			String aasJson = S3Helper.getBaSyxObjectContent(s3Client, os.getBucketName(), os.getKey());
			AssetAdministrationShell aas = AssetAdministrationShell
					.createAsFacade((Map<String, Object>) gsonTools.deserialize(aasJson));
			S3AASAPI aasApi = (S3AASAPI) aasApiProvider.getAASApi(aas);
			MultiSubmodelProvider provider = createMultiSubmodelProvider(aasApi);
			addSubmodelsFromDB(provider, aas);
			aasProviderMap.put(aas.getIdentification().getId(), provider);
		}
	}

	private void addSubmodelsFromDB(MultiSubmodelProvider provider, AssetAdministrationShell aas) {
		Collection<IReference> submodelRefs = aas.getSubmodelReferences();
		List<String> smIds = new ArrayList<>();
		List<String> smIdShorts = new ArrayList<>();
		for (IReference ref : submodelRefs) {
			List<IKey> keys = ref.getKeys();
			IKey lastKey = keys.get(keys.size() - 1);
			if (lastKey.getIdType() == KeyType.IDSHORT) {
				smIdShorts.add(lastKey.getValue());
			} else {
				smIds.add(lastKey.getValue());
			}
		}

		// Add submodel ids by id shorts
		for (String idShort : smIdShorts) {
			String id = getSubmodelId(idShort);
			if (id != null) {
				smIds.add(id);
			}
		}

		for (String id : smIds) {
			addSubmodelProvidersById(id, provider);
		}
	}
	
	private String getSubmodelId(String idShort) {
		List<S3ObjectSummary> osList = s3Client.listObjectsV2(submodelBucketName).getObjectSummaries();
		for(S3ObjectSummary os : osList) {
			try {
				S3Object smObject = s3Client.getObject(new GetObjectRequest(submodelBucketName, os.getKey()));
				String idshortValue = (String)(smObject.getObjectMetadata().getRawMetadataValue(IDSHORT_TAG));
				
				if(idshortValue.equals(idShort)) {
					return (String)(smObject.getObjectMetadata().getRawMetadataValue(IDENTIFIER_TAG));
				}
				smObject.close();
			}catch(AmazonS3Exception e) {
				System.err.println("Cannot find the data object "+ os.getKey());
			}catch(IOException e) {
				e.printStackTrace();
				System.err.println("Cannot close the fetched data obejct");
			}
		}
		return null;
	}

	private void addSubmodelProvidersById(String smId, MultiSubmodelProvider provider) {
		ISubmodelAPI smApi = new S3SubmodelAPI(s3Client, submodelBucketName, smId);
		SubmodelProvider smProvider = new SubmodelProvider(smApi);
		provider.addSubmodel(smProvider);
	}

	private MultiSubmodelProvider createMultiSubmodelProvider(IAASAPI aasApi) {
		AASModelProvider aasProvider = new AASModelProvider(aasApi);
		IConnectorFactory connProvider = new HTTPConnectorFactory();

		ISubmodelAggregator usedAggregator = submodelAggregatorFactory.create();

		return new MultiSubmodelProvider(aasProvider, registry, connProvider, aasApiProvider, usedAggregator);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<IAssetAdministrationShell> getAASList() {
		return aasProviderMap.values().stream().map(p -> {
			try {
				return p.getValue("/aas");
			} catch (Exception e1) {
				e1.printStackTrace();
				throw new RuntimeException();
			}
		}).map(m -> {
			AssetAdministrationShell aas = new AssetAdministrationShell();
			aas.putAll((Map<? extends String, ? extends Object>) m);
			return aas;
		}).collect(Collectors.toList());
	}

	@Override
	public IAssetAdministrationShell getAAS(IIdentifier aasId) throws ResourceNotFoundException {
		IModelProvider aasProvider = getAASProvider(aasId);

		@SuppressWarnings("unchecked")
		Map<String, Object> aasMap = (Map<String, Object>) aasProvider.getValue("/aas");
		return AssetAdministrationShell.createAsFacade(aasMap);
	}

	@Override
	public IModelProvider getAASProvider(IIdentifier aasId) throws ResourceNotFoundException {
		MultiSubmodelProvider provider = aasProviderMap.get(aasId.getId());
		if (provider == null) {
			throw new ResourceNotFoundException("AAS with Id " + aasId.getId() + " does not exist");
		}
		return provider;
	}

	@Override
	public void createAAS(AssetAdministrationShell aas) {
		IAASAPI aasApi = this.aasApiProvider.create(aas);
		MultiSubmodelProvider provider = createMultiSubmodelProvider(aasApi);
		aasProviderMap.put(aas.getIdentification().getId(), provider);
	}

	@Override
	public void updateAAS(AssetAdministrationShell aas) throws ResourceNotFoundException {
		MultiSubmodelProvider oldProvider = (MultiSubmodelProvider) getAASProvider(aas.getIdentification());
		IAASAPI aasApi = aasApiProvider.create(aas);
		AASModelProvider contentProvider = new AASModelProvider(aasApi);
		IConnectorFactory connectorFactory = oldProvider.getConnectorFactory();

		MultiSubmodelProvider updatedProvider = new MultiSubmodelProvider(contentProvider, registry, connectorFactory,
				aasApiProvider, oldProvider.getSmAggregator());

		aasProviderMap.put(aas.getIdentification().getId(), updatedProvider);
		logger.info("update aas with id {}", aas.getIdentification().getId());
	}

	@Override
	public void deleteAAS(IIdentifier aasId) {
		if (!S3Helper.isVersionedBucket(s3Client, aasBucketName)) {
			try {
				s3Client.deleteObject(new DeleteObjectRequest(aasBucketName, aasId.getId()));
			} catch (AmazonS3Exception e) {
				throw new ResourceNotFoundException("No aas found with id " + aasId.getId());
			}
			return;
		}

		VersionListing versionListing = S3Helper.listBucketVersions(s3Client, aasBucketName);
		while (true) {
			for (Iterator<?> iterator = versionListing.getVersionSummaries().iterator(); iterator.hasNext();) {
				S3VersionSummary versionSummary = (S3VersionSummary) iterator.next();
				String key = versionSummary.getKey();
				if (key.equals(aasId.getId())) {
					String versionId = versionSummary.getVersionId();
					s3Client.deleteVersion(aasBucketName, key, versionId);
				}
			}

			if (versionListing.isTruncated()) {
				versionListing = s3Client.listNextBatchOfVersions(versionListing);
			} else {
				break;
			}
		}
		aasProviderMap.remove(aasId.getId());
	}

	public void reset() {
		deleteBucket(s3Client, aasBucketName);
		deleteBucket(s3Client, submodelBucketName);
		aasProviderMap.clear();
	}

	private void deleteBucket(AmazonS3 s3Client, String bucketName) {
		if (S3Helper.isVersionedBucket(s3Client, bucketName)) {
			S3Helper.wipeVersionedBucket(s3Client, bucketName);
			return;
		}
		S3Helper.wipeUnversionedBucket(s3Client, bucketName);
	}
}
