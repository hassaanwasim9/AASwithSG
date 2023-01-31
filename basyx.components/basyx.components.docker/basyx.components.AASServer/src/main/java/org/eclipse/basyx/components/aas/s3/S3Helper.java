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
package org.eclipse.basyx.components.aas.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.eclipse.basyx.components.aas.configuration.BaSyxS3Configuration;
import org.eclipse.basyx.components.aas.submodelserializer.SubmodelSerializer;
import org.eclipse.basyx.submodel.metamodel.api.ISubmodel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;
import com.google.gson.Gson;

/**
 * Helper class for common Amazon S3 features
 * 
 * @author jungjan
 */
public class S3Helper {
	private static Logger logger = LoggerFactory.getLogger(S3Helper.class);

	public static AmazonS3 createS3Client(BaSyxS3Configuration config) {
		handleCertCheckingPolicy(config);
		logger.info("Create AmazonS3 client...");
		EndpointConfiguration endpointConfiguration = createEndpointConfiguration(config);
		AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(endpointConfiguration).withCredentials(createCredentials(config));

		if (isPathStyleAccessEnabled(config)) {
			// needed for testing with io.findify.s3mock.S3Mock
			amazonS3ClientBuilder.withPathStyleAccessEnabled(true);
		}
		logger.info("Created S3 client for {}:{}.", config.getServiceEndpointHost(), config.getServiceEndpointPort());
		return amazonS3ClientBuilder.build();
	}

	/**
	 * Create a S3 client with versioning enabled
	 * @param s3Client
	 * @param bucketName
	 */
	public static void createBucketIfNotExists(AmazonS3 s3Client, String bucketName) {
		checkS3Compliance(bucketName);

		if (s3Client.doesBucketExistV2(bucketName)) {
			logger.info("Using existing Bucket '{}'.", bucketName);
			return;
		}
		try {
			s3Client.createBucket(bucketName);
			BucketVersioningConfiguration versionConfig = new BucketVersioningConfiguration();
			versionConfig.setStatus(BucketVersioningConfiguration.ENABLED);
			SetBucketVersioningConfigurationRequest bvr = new SetBucketVersioningConfigurationRequest(bucketName, versionConfig);
			s3Client.setBucketVersioningConfiguration(bvr);
			logger.info("Enable versioning of bucket {}", bucketName);
			
		} catch (AmazonS3Exception e) {
			logger.error(e.getErrorMessage());
		}
	}

	public static void deleteBucketIfExists(AmazonS3 s3Client, String bucketName) {
		if (!s3Client.doesBucketExistV2(bucketName)) {
			logger.info("Bucket '{}' could not be deleted since it does not exist.");
			return;
		}

		if (!isBucketBlank(s3Client, bucketName)) {
			if (isVersionedBucket(s3Client, bucketName)) {
				wipeVersionedBucket(s3Client, bucketName);
			} else {
				wipeUnversionedBucket(s3Client, bucketName);
			}
		}
		s3Client.deleteBucket(bucketName);
		logger.info("Deleted bucket '{}'.", bucketName);
	}

	public static void wipeUnversionedBucket(AmazonS3 s3Client, String bucketName) {
		ObjectListing object_listing = s3Client.listObjects(bucketName);
		while (true) {
			for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
				S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
				String key = summary.getKey();
				deleteObjectFromUnversionedBucket(s3Client, bucketName, key);
			}

			if (object_listing.isTruncated()) {
				object_listing = s3Client.listNextBatchOfObjects(object_listing);
			} else {
				break;
			}
		}
	}

	public static void wipeVersionedBucket(AmazonS3 s3Client, String bucketName) {
		VersionListing version_listing = listBucketVersions(s3Client, bucketName);
		while (true) {
			for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();) {
				S3VersionSummary versionSummary = (S3VersionSummary) iterator.next();
				String key = versionSummary.getKey();
				String versionId = versionSummary.getVersionId();
				deleteObjectFromVersionedBucket(s3Client, bucketName, key, versionId);
			}

			if (version_listing.isTruncated()) {
				version_listing = s3Client.listNextBatchOfVersions(version_listing);
			} else {
				break;
			}
		}
	}

	public static void deleteObjectFromUnversionedBucket(AmazonS3 s3Client, String bucketName, String key) {
		s3Client.deleteObject(bucketName, key);
		logger.info("Removed object '{}' from bucket '{}'.", key, bucketName);
	}

	public static void deleteObjectFromVersionedBucket(AmazonS3 s3Client, String bucketName, String key,
			String versionId) {
		s3Client.deleteVersion(bucketName, key, versionId);
		logger.info("Removed version '{}' of object '{}' from bucket '{}'.", versionId, key, bucketName);
	}

	private static void handleCertCheckingPolicy(BaSyxS3Configuration config) {
		if (config.getDiableCertChecking() == null) {
			return;
		}
		if (config.getDiableCertChecking().equals(String.valueOf(true))) {
			System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		}
	}

	private static boolean isPathStyleAccessEnabled(BaSyxS3Configuration config) {
		if (config.getPathStyleAccessEnabled() == null)
			return false;
		return Boolean.parseBoolean(config.getPathStyleAccessEnabled());
	}

	private static AWSCredentialsProvider createCredentials(BaSyxS3Configuration config) {
		if (hasCredentials(config)) {
			logger.info("Using BasicAWSCredentials (access key, secret key)...");
			return new AWSStaticCredentialsProvider(
					new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey()));
		}
		logger.info("Using AnonymousAWSCredentials...");
		return new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
	}

	private static boolean hasCredentials(BaSyxS3Configuration config) {
		return !(config.getAccessKey() == null || config.getSecretKey() == null);
	}

	private static EndpointConfiguration createEndpointConfiguration(BaSyxS3Configuration config) {
		return new EndpointConfiguration(createServiceEndpoint(config), config.getSigningRegion());
	}

	private static String createServiceEndpoint(BaSyxS3Configuration config) {
		String host = config.getServiceEndpointHost();
		String port = config.getServiceEndpointPort();
		return port != null ? host + ":" + port : host;
	}

	private static boolean isBucketBlank(AmazonS3 s3Client, String bucketName) {
		return listBucketContentKeys(s3Client, bucketName) == null
				|| listBucketContentKeys(s3Client, bucketName).size() == 0;
	}

	public static boolean isVersionedBucket(AmazonS3 s3Client, String bucketName) {
		return listBucketVersions(s3Client, bucketName) != null;
	}

	public static VersionListing listBucketVersions(AmazonS3 s3Client, String bucketName) {
		return s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
	}

	/**
	 * Get an AAS or Submodel in JSON format
	 * @param s3Client
	 * @param bucketName 
	 * @param key - id of aas or submodel
	 * @return - aas or submodel in json string
	 * @throws IOException
	 */
	public static String getBaSyxObjectContent(AmazonS3 s3Client, String bucketName, String key) throws IOException {
		S3Object fullObject;
		try {
			fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			byte[] objectJsonBytes = fullObject.getObjectContent().readAllBytes();
			String jsonString = new String(objectJsonBytes);
			fullObject.close();
			return jsonString;
		} catch (AmazonServiceException e) {
			return null;
        } 
	}

	/**
	 * Uploads a json serialized submodel to a given S3 bucket. Currently, the
	 * submodel is carried by an otherwise empty AAS. This is done to profit from
	 * existing methods.
	 * 
	 * @param s3Client
	 * @param bucketName
	 * @param submodel
	 * @param key
	 */
	public static void uploadSubmodelToBucket(AmazonS3 s3Client, String bucketName, ISubmodel submodel, String key) {
		//key = listBucketContentKeys(s3Client, bucketName).contains(key) ? key + ".update" : key;
		String jsonSubmodel = SubmodelSerializer.serialize(submodel);
		ObjectMetadata metadata = addSubmodelMetadata(submodel);

		try {
			s3Client.putObject(bucketName, key, new ByteArrayInputStream(jsonSubmodel.getBytes()), metadata);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		}
	}

	public static String downloadBucketContentByKey(AmazonS3 s3Client, String bucketName, String key) {
		S3Object object = s3Client.getObject(bucketName, key);
		try (S3ObjectInputStream s3ObjectInputStream = object.getObjectContent()) {
			String content = new BufferedReader(new InputStreamReader(s3ObjectInputStream, StandardCharsets.UTF_8))
					.lines().collect(Collectors.joining("\n"));
			logger.info("Downloaded content of bucket '{}' for key '{}':\n{}.", bucketName, key, content);
			return content;
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return null;
	}

	public static List<String> downloadBucketContentAsList(AmazonS3 s3Client, String bucketName) {
		logger.info("Downloading content of bucket '{}'...", bucketName);
		return listBucketContentKeys(s3Client, bucketName).stream()
				.map(key -> downloadBucketContentByKey(s3Client, bucketName, key)).collect(Collectors.toList());
	}

	public static String downloadObjectMetadataByKey(AmazonS3 s3Client, String bucketName, String key) {
		S3Object object = s3Client.getObject(bucketName, key);
		ObjectMetadata metadata = object.getObjectMetadata();

		logger.info("Downloaded metadata of bucket '{}' for key '{}': {}.", bucketName, key, metadata);
		return new Gson().toJson(metadata);
	}

	public static List<Bucket> listBuckets(AmazonS3 s3Client) {
		try {
			return s3Client.listBuckets();
		} catch (NullPointerException e) {
			logger.warn(e.getMessage());
			e.printStackTrace();
		}
		return new LinkedList<Bucket>();
	}

	public static List<String> listBucketNames(AmazonS3 s3Client) {
		List<Bucket> buckets = listBuckets(s3Client);
		return buckets.stream().map(bucket -> bucket.getName()).collect(Collectors.toList());
	}

	public static List<String> listBucketContentKeys(AmazonS3 s3Client, String bucketName) {
		ListObjectsV2Result result = s3Client.listObjectsV2(bucketName);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		return objects.stream().map(object -> object.getKey()).collect(Collectors.toList());
	}

	private static ObjectMetadata addSubmodelMetadata(ISubmodel submodel) {
		ObjectMetadata metadata = createMetadata();
		if (submodel.getSemanticId() != null) {
			String semanticId = SubmodelSerializer.serializeSemantiIdAsS3Metadata(submodel);
			metadata.addUserMetadata("semanticId", semanticId);
		}
		String identificationId = submodel.getIdentification().getId();

		metadata.addUserMetadata("identifier", identificationId);
		metadata.addUserMetadata("idShort", submodel.getIdShort());
		return metadata;
	}

	public static ObjectMetadata createMetadata() {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("application/json");
		metadata.addUserMetadata("timestamp", getTimeStamp());

		return metadata;
	}

	/**
	 * Generates an Amazon S3 compliant bucket name from a given bucketName
	 * 
	 * If no bucketName (null) is given, a default bucket name will be set,
	 * following the schema: {@code <yyyyMMddHHmmssSSS>-basyx-s3-bucket}.
	 * 
	 * @param bucketName
	 * @return
	 */
	public static String makeBucketName(String bucketName) {
		logger.info("Creating bucket name from {}{}...", "'" + bucketName + "'",
				bucketName == null ? ". (Using default bucket name)" : "");
		bucketName = bucketName != null ? bucketName : getTimeStamp() + "-basyx-s3-bucket";
		bucketName = makeS3Compliant(bucketName);
		logger.info("S3 bucket will be named: '{}'.", bucketName);
		return bucketName;
	}

	/**
	 * Ensures that the name of an S3 bucket is compliant with
	 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
	 * 
	 * @param bucketName
	 * @return
	 */
	private static String makeS3Compliant(String bucketName) {
		bucketName = replaceWhitespace(bucketName, "-");
		bucketName = cut(bucketName, 63);
		bucketName = bucketNameToLowerCase(bucketName);
		checkS3Compliance(bucketName);
		return bucketName;
	}

	private static void checkS3Compliance(String bucketName) {
		String message = "S3 Bucket names must ";
		if (!isFirstCharacterValid(bucketName)) {
			message += "begin and end with a letter or number.";
			logErrorAndThrowAmazonServiceException(message);
		}
		if (stringIsFormattedAsAnIPAddress(bucketName)) {
			message += "not be formatted as an IP address (for example, 192.168.5.4).";
			logErrorAndThrowAmazonServiceException(message);
		}
		if (bucketName.contains("..")) {
			message += "not contain two adjacent periods.";
			logErrorAndThrowAmazonServiceException(message);
		}
		if (bucketName.startsWith("xn--")) {
			message += "not start with the prefix 'xn--'.";
			logErrorAndThrowAmazonServiceException(message);
		}
		if (bucketName.endsWith("-s3alias")) {
			message += "not end with the suffix '-s3alias'. This suffix is reserved for access point alias names. For more information, see Using a bucket-style alias for your access point.";
			logErrorAndThrowAmazonServiceException(message);
		}
		checkBucketNameForInvalidSymbols(bucketName);
	}

	private static void checkBucketNameForInvalidSymbols(String bucketName) {
		String validSymbolsPattern = "^([a-z]|\\d|\\.|-)+$";
		String validSymbolPattern = "([a-z]|\\d|\\.|-)";
		if (!bucketName.matches(validSymbolsPattern)) {
			List<Character> invalidSymbols = bucketName.chars().distinct().mapToObj(c -> (char) c)
					.filter(c -> !Character.toString(c).matches(validSymbolPattern)).sorted()
					.collect(Collectors.toList());
			String message = "S3 Bucket names can consist only of lowercase letters (a-z), numbers, dots (.), and hyphens (-);\nInvalid Symbols: "
					+ invalidSymbols.toString();
			logErrorAndThrowAmazonServiceException(message);
		}
	}

	private static String bucketNameToLowerCase(String bucketName) {
		if (stringContainsUpperCase(bucketName)) {
			logger.info(
					"S3 Bucket names can consist only of lowercase letters (a-z), numbers, dots (.), and hyphens (-).");
			logger.info("Fixed: -> S3 Bucket name put to lower case.");
			return bucketName.toLowerCase();
		}
		return bucketName;
	}

	private static String cut(String strToBeModified, int maxLength) {
		if (strToBeModified.length() > maxLength) {
			logger.info("S3 Bucket names must be between 3 (min) and 63 (max) characters long.");
			logger.info("Fixed: -> Cut bucket name to meet Amazon S3 compatibility");
			return strToBeModified.substring(0, maxLength - 1);
		}
		return strToBeModified;
	}

	private static String replaceWhitespace(String str, String replacement) {
		String whiteSpavePattern = "\\s+";
		Pattern pattern = Pattern.compile(whiteSpavePattern);
		Matcher matcher = pattern.matcher(str);
		if (matcher.find()) {
			logger.info("S3 Bucket names must not contain whitespace");
			logger.info("Fixed: -> Replaced whitespace in bucket name with '{}'.", replacement);
			return str.replaceAll(whiteSpavePattern, replacement);
		}
		return str;
	}

	private static boolean stringContainsUpperCase(String str) {
		String upperCasePattern = "[A-Z]";
		Pattern pattern = Pattern.compile(upperCasePattern);
		Matcher matcher = pattern.matcher(str);
		return matcher.find();
	}

	private static boolean stringIsFormattedAsAnIPAddress(String str) {
		String ipAddressPattern = "^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)(\\.(?!$)|$)){4}$";
		return str.matches(ipAddressPattern);
	}

	private static boolean isFirstCharacterValid(String bucketName) {
		String validPattern = "^([a-z]|\\d).*([a-z]|\\d)$";
		return bucketName.matches(validPattern);
	}

	private static String getTimeStamp() {
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
	}

	private static void logErrorAndThrowAmazonServiceException(String message) {
		logger.error(message);
		throw new AmazonServiceException(message);
	}

}
