/*******************************************************************************
 * Copyright (C) 2022 the Eclipse BaSyx Authors
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
package org.eclipse.basyx.components.aas.configuration;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.basyx.components.aas.s3.S3Helper;
import org.eclipse.basyx.components.configuration.BaSyxConfiguration;

/**
 * Represents a simple BaSyx S3 configuration for an AAS Server that can be
 * loaded from a properties file.
 * 
 * Disclaimer: This configuration class is in a prototypical stage. Loading from
 * file or Environment is not (yet) supported. Default values are not provided.
 * At least the properties SERVICE_ENDPOINT_HOST, SERVICE_ENDPOINT_PORT,
 * SIGNING_REGION, and BUCKET_NAME must be set to successfully build a S3
 * connection from this file, either by constructor call or using the setters.
 * 
 * @author jungjan
 *
 */
public class BaSyxS3Configuration extends BaSyxConfiguration {

	// Properties
	public static final String SERVICE_ENDPOINT_HOST = "serviceEndpointHost";
	public static final String SERVICE_ENDPOINT_PORT = "serviceEndpointPort";
	public static final String SIGNING_REGION = "signingRegion";
	public static final String ACCESS_KEY = "accessKey";
	public static final String SECRET_KEY = "secretKey";
	public static final String AAS_BUCKET_NAME = "aasBucketName";
	public static final String SUBMODEL_BUCKET_NAME = "submodelBucketName";
	public static final String DISABLE_CERT_CHECKING = "diableCertChecking";
	public static final String PATH_STYLE_ACCESS_ENABLED = "pathStyleAccessEnabled";

	public BaSyxS3Configuration(Map<String, String> values) {
		super(values);
	}

	public BaSyxS3Configuration(String serviceEndpointHost, String serviceEndpointPort, String signingRegion) {
		super(new HashMap<>());
		this.setServiceEndpointHost(serviceEndpointHost);
		this.setServiceEndpointPort(serviceEndpointPort);
		this.setSigningRegion(signingRegion);
	}

//	public BaSyxS3Configuration(String serviceEndpointHost, String serviceEndpointPort, String signingRegion,
//			String aasBucketName, String submodelBucketName) {
//		super(new HashMap<>());
//		this.setServiceEndpointHost(serviceEndpointHost);
//		this.setServiceEndpointPort(serviceEndpointPort);
//		this.setSigningRegion(signingRegion);
//		this.setAASBucketName(aasBucketName);
//		this.setSubmodelBucketName(submodelBucketName);
//	}

	public BaSyxS3Configuration(String serviceEndpointHost, String serviceEndpointPort, String signingRegion,
			String accessKey, String secretKey) {
		super(new HashMap<>());
		this.setServiceEndpointHost(serviceEndpointHost);
		this.setServiceEndpointPort(serviceEndpointPort);
		this.setSigningRegion(signingRegion);
		this.setAccessKey(accessKey);
		this.setSecretKey(secretKey);
	}

	public BaSyxS3Configuration(String serviceEndpointHost, String serviceEndpointPort, String signingRegion,
			String accessKey, String secretKey, String aasBucketName, String submodelBucketName) {
		super(new HashMap<>());
		this.setServiceEndpointHost(serviceEndpointHost);
		this.setServiceEndpointPort(serviceEndpointPort);
		this.setSigningRegion(signingRegion);
		this.setAccessKey(accessKey);
		this.setSecretKey(secretKey);
		this.setAASBucketName(aasBucketName);
		this.setSubmodelBucketName(submodelBucketName);
	}

	public String getServiceEndpointHost() {
		return getProperty(SERVICE_ENDPOINT_HOST);
	}

	public String getServiceEndpointPort() {
		return getProperty(SERVICE_ENDPOINT_PORT);
	}

	public String getSigningRegion() {
		return getProperty(SIGNING_REGION);
	}

	public String getAccessKey() {
		return getProperty(ACCESS_KEY);
	}

	public String getSecretKey() {
		return getProperty(SECRET_KEY);
	}

//	/**
//	 * If this method is called without a bucketName being set beforehand, a default
//	 * bucket name will be returned. (see {@link #setBucketName(String bucketName)})
//	 * 
//	 * @return
//	 */
	public String getAASBucketName() {
		if (getProperty(AAS_BUCKET_NAME) == null) {
			setAASBucketName(null);
		}
		return getProperty(AAS_BUCKET_NAME);
	}
	
	public String getSubmodelBucketName() {
		if (getProperty(SUBMODEL_BUCKET_NAME) == null) {
			setSubmodelBucketName(null);
		}
		return getProperty(SUBMODEL_BUCKET_NAME);
	}

	public String getDiableCertChecking() {
		return getProperty(DISABLE_CERT_CHECKING);
	}

	/**
	 * Disclaimer: Needed for testing with io.findify.s3mock.S3Mock
	 * 
	 * @return
	 */
	public String getPathStyleAccessEnabled() {
		return getProperty(PATH_STYLE_ACCESS_ENABLED);
	}

	public void setServiceEndpointHost(String host) {
		setProperty(SERVICE_ENDPOINT_HOST, host);
	}

	public void setServiceEndpointPort(String port) {
		setProperty(SERVICE_ENDPOINT_PORT, port);
	}

	public void setSigningRegion(String signingRegion) {
		setProperty(SIGNING_REGION, signingRegion);
	}

	public void setAccessKey(String accessKey) {
		setProperty(ACCESS_KEY, accessKey);
	}

	public void setSecretKey(String secretKey) {
		setProperty(SECRET_KEY, secretKey);
	}

//	/**
//	 * Sets an Amazon S3 compliant bucket name. The method
//	 * S3Helper.makeBucketName(bucketName)} ensures that the bucket name
//	 * will be Amazon S3 compliant and, if necessary, modifies the name accordingly
//	 * 
//	 * If no bucketName (null) is given, a default bucket name will be set,
//	 * following the schema: {@code <yyyyMMddHHmmssSSS>-basyx-s3-bucket}.
//	 * 
//	 * @param bucketName
//	 */
	public void setAASBucketName(String aasBucketName) {
		setProperty(AAS_BUCKET_NAME, S3Helper.makeBucketName(aasBucketName));
	}
	
	public void setSubmodelBucketName(String submodelBucketName) {
		setProperty(SUBMODEL_BUCKET_NAME, S3Helper.makeBucketName(submodelBucketName));
	}

	public void setDiableCertChecking(boolean diableCertChecking) {
		setProperty(DISABLE_CERT_CHECKING, Boolean.toString(diableCertChecking));
	}

	/**
	 * Disclaimer: needed for testing with io.findify.s3mock.S3Mock
	 * 
	 * @param pathStyleAccessEnabled
	 */
	public void setPathStyleAccessEnabled(boolean pathStyleAccessEnabled) {
		setProperty(PATH_STYLE_ACCESS_ENABLED, Boolean.toString(pathStyleAccessEnabled));
	}

}
