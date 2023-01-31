/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.basyx.regression.AASServer.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Executable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.poi.util.LittleEndianInputStream;
import org.eclipse.basyx.aas.manager.ConnectedAssetAdministrationShellManager;
import org.eclipse.basyx.aas.metamodel.map.AssetAdministrationShell;
import org.eclipse.basyx.aas.metamodel.map.descriptor.CustomId;
import org.eclipse.basyx.aas.metamodel.map.descriptor.ModelUrn;
import org.eclipse.basyx.aas.registration.api.IAASRegistry;
import org.eclipse.basyx.aas.registration.memory.InMemoryRegistry;
import org.eclipse.basyx.components.aas.AASServerComponent;
import org.eclipse.basyx.components.aas.configuration.AASServerBackend;
import org.eclipse.basyx.components.aas.configuration.BaSyxAASServerConfiguration;
import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.components.configuration.BaSyxContextConfiguration;
import org.eclipse.basyx.regression.AASServer.AASServerSuite;
import org.eclipse.basyx.submodel.metamodel.api.ISubmodel;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IIdentifier;
import org.eclipse.basyx.submodel.metamodel.api.submodelelement.dataelement.IBlob;
import org.eclipse.basyx.submodel.metamodel.map.Submodel;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.dataelement.Blob;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.dataelement.property.Property;
import org.eclipse.basyx.vab.coder.json.serialization.DefaultTypeFactory;
import org.eclipse.basyx.vab.coder.json.serialization.GSONTools;
import org.eclipse.basyx.vab.protocol.api.IConnectorFactory;
import org.eclipse.basyx.vab.protocol.http.connector.HTTPConnectorFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.util.IOUtils;

						
/**
 * Testing that the AAS Server component s3 as submodel storage
 * 
 * @author Hassaan
 *
 */
public class AASWithSG extends AASServerSuite {
	private static final String AAS_BUCKET_NAME = "aas-bucket4";
	private static final String SUBMODEL_BUCKET_NAME = "submodel-bucket4";
	private static AASServerComponent component;
	private static BaSyxS3Configuration s3Config;
	private static AmazonS3 s3Client;
	private static IAASRegistry registry;
	protected static ConnectedAssetAdministrationShellManager manager;
	private GSONTools gsonTools = new GSONTools(new DefaultTypeFactory());

	@BeforeClass
	public static void setUpClass() throws ParserConfigurationException, SAXException, IOException {
		BaSyxContextConfiguration contextConfig = new BaSyxContextConfiguration();
		contextConfig.loadFromResource(BaSyxContextConfiguration.DEFAULT_CONFIG_PATH);
		
		Map<String, String> s3ConfigurationValues = new HashMap<>();

			
		/* S3 Netapp Configuration Values */
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_HOST, "");
		s3ConfigurationValues.put(BaSyxS3Configuration.SERVICE_ENDPOINT_PORT, "");
		s3ConfigurationValues.put(BaSyxS3Configuration.SIGNING_REGION, "");
		s3ConfigurationValues.put(BaSyxS3Configuration.ACCESS_KEY, "");
		s3ConfigurationValues.put(BaSyxS3Configuration.SECRET_KEY, "");

		
		s3Config = new BaSyxS3Configuration(s3ConfigurationValues);
		s3Config.setDiableCertChecking(true);
		s3Client = S3Helper.createS3Client(s3Config);
		
		deleteExistingBucketsAndObjects();
		S3Helper.createBucketIfNotExists(s3Client, AAS_BUCKET_NAME);
		S3Helper.createBucketIfNotExists(s3Client, SUBMODEL_BUCKET_NAME);
		
		s3Config.setAASBucketName(AAS_BUCKET_NAME);
		s3Config.setSubmodelBucketName(SUBMODEL_BUCKET_NAME);
		
		component = new AASServerComponent(contextConfig, new BaSyxAASServerConfiguration(AASServerBackend.S3, ""), s3Config);
		
		registry = new InMemoryRegistry();
		IConnectorFactory connectorFactory = new HTTPConnectorFactory();
		manager = new ConnectedAssetAdministrationShellManager(registry, connectorFactory);
		component.setRegistry(registry);
		
		component.startComponent();
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

	@Before
	public void setUp() {
		super.setUp();
	}
	
	
	@AfterClass
	public static void tearDownClass() {
		component.stopComponent();
//		S3Helper.deleteBucketIfExists(s3Client, s3Config.getAASBucketName());
//		S3Helper.deleteBucketIfExists(s3Client, s3Config.getSubmodelBucketName());
	}
	
	@Test
	public void addFilesToSubmodel() throws Exception {
		IIdentifier carrierShellIdentifier = new CustomId("carrierBridge");
		IIdentifier submodelIdentifier1 = new CustomId("Bridge_123004");  // Change to new Bridge ID

		createCarrierShell(carrierShellIdentifier);
		
		Submodel submodel1 = createSubmodelBridge(submodelIdentifier1.getId(), submodelIdentifier1);
		
//		Pushing Submodel to S3
		manager.createSubmodel(carrierShellIdentifier, submodel1);		
			
	}
	
	
	protected Submodel createSubmodelBridge(String idShort, IIdentifier submodelIdentifier) {
		
		// - Create property
		String fileName = "bridge_123004.csv"; // Change to new fileName
		String bucketName = "bridge-data";	
			
		
		
		Submodel SubModelBridge = new Submodel();
		SubModelBridge.setIdShort(idShort);
		SubModelBridge.setIdentification(submodelIdentifier);
      
		Property bridgedata = new Property("bridge-data", fileName);
		Property bucketname= new Property("bucket-name", bucketName);
			
		// Adding Properties to the submodel
        SubModelBridge.addSubmodelElement(bucketname);
        SubModelBridge.addSubmodelElement(bridgedata);
        
        return SubModelBridge;
	}
	
	
	
	
	/**
	 * Submodel Updates currently result in adding the updated submodel to the existing ones in the Bucket
	 * In order to continue to have unique keys, the key of the updated submodes is expected to be suffixed with ".update".
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateSubmodel() throws Exception {
		IIdentifier carrierShellIdentifier = new CustomId("carrierShell");
		IIdentifier submodelIdentifier = new CustomId("S3submodelId123");
		createCarrierShell(carrierShellIdentifier);
		
		Submodel submodel = createSubmodel(submodelIdentifier.getId(), submodelIdentifier);
		
		// create the submodel in the db
		manager.createSubmodel(carrierShellIdentifier, submodel);
		
		// retrieve the submodel and add an submodel element
		Property property = new Property("prop001", 456);
		submodel.addSubmodelElement(property);
		
		// update the submodel in db
		manager.createSubmodel(carrierShellIdentifier, submodel);

	}
	

	private void createCarrierShell(IIdentifier shellIdentifierForSubmodel) {
		AssetAdministrationShell shell = createShell(shellIdentifierForSubmodel.getId(), shellIdentifierForSubmodel);
		manager.createAAS(shell, getURL());
	}

	@Override
	protected String getURL() {
		return component.getURL();
	}

}
