package org.eclipse.basyx.components.aas.s3;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.basyx.aas.metamodel.api.IAssetAdministrationShell;
import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.restapi.api.IAASAPI;
import org.eclipse.basyx.submodel.metamodel.api.reference.IKey;
import org.eclipse.basyx.submodel.metamodel.api.reference.IReference;
import org.eclipse.basyx.vab.coder.json.serialization.DefaultTypeFactory;
import org.eclipse.basyx.vab.coder.json.serialization.GSONTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;


/**
 * Implementation of the IAASAPI with Amazon S3 features
 * 
 * @author zhangzai
 *
 */
public class S3AASAPI implements IAASAPI {
	private static Logger logger = LoggerFactory.getLogger(S3AASAPI.class);
	private static final String IDENTIFIER_TAG = "identifier";
	private static final String IDSHORT_TAG = "idShort";
	
	private AmazonS3 s3Client;
	private String bucketName;
	protected String aasId;
	protected GSONTools gsonTools = new GSONTools(new DefaultTypeFactory());
	
	public S3AASAPI( AmazonS3 s3Client, String bucketName, String aasId) {
		super();
		this.s3Client = s3Client;
		this.bucketName = bucketName;
		this.aasId = aasId;
	}

	@SuppressWarnings("unchecked")
	@Override
	public IAssetAdministrationShell getAAS() {
		String aasJson="";
		try {
			logger.info(String.format("get aas with id %s ", aasId));
			aasJson = S3Helper.getBaSyxObjectContent(s3Client, bucketName, aasId);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return AssetAdministrationShell.createAsFacade((Map<String, Object>)gsonTools.deserialize(aasJson));
	}

	@Override
	public void addSubmodel(IReference submodel) {
		AssetAdministrationShell aas = (AssetAdministrationShell)this.getAAS();
		aas.addSubmodelReference(submodel);
		uploadAASToBucket(s3Client, bucketName, aas, aasId);
	}

	@Override
	public void removeSubmodel(String id) {
		AssetAdministrationShell aas = (AssetAdministrationShell)this.getAAS();
		Collection<IReference> submodelReferences = aas.getSubmodelReferences();
		for (Iterator<IReference> iterator = submodelReferences.iterator(); iterator.hasNext();) {
			IReference ref = iterator.next();
			List<IKey> keys = ref.getKeys();
			IKey lastKey = keys.get(keys.size() - 1);
			String idValue = lastKey.getValue();
			if (idValue.equals(id)) {
				iterator.remove();
				break;
			}
		}
		aas.setSubmodelReferences(submodelReferences);
		uploadAASToBucket(s3Client, bucketName, aas, aasId);
	}

	
	public void setAAS(AssetAdministrationShell aas) {
		aasId = aas.getIdentification().getId();
		uploadAASToBucket(s3Client, bucketName, aas, aasId);
	}
	
	private void uploadAASToBucket(AmazonS3 s3Client, String bucketName, IAssetAdministrationShell aas, String key) {
		String jsonAAS = (new GSONTools(new DefaultTypeFactory())).serialize(aas);
		ObjectMetadata metadata = addAASMetadata(aas);
		try {
			s3Client.putObject(bucketName, key, new ByteArrayInputStream(jsonAAS.getBytes()),
					metadata);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		}
	}
	
	private ObjectMetadata addAASMetadata(IAssetAdministrationShell aas) {
		ObjectMetadata metadata = S3Helper.createMetadata();
		
		metadata.addUserMetadata(IDENTIFIER_TAG, aasId);
		metadata.addUserMetadata(IDSHORT_TAG, aas.getIdShort());
		return metadata;
	}
	
}
