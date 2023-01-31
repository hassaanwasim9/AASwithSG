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
package org.eclipse.basyx.components.aas.s3;


import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.basyx.submodel.aggregator.SubmodelAggregator;
import org.eclipse.basyx.submodel.metamodel.api.ISubmodel;
import org.eclipse.basyx.submodel.metamodel.api.identifier.IIdentifier;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPIFactory;
import org.eclipse.basyx.vab.exception.provider.ResourceNotFoundException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3VersionSummary;

/**
 * Implementation of the ISubmodelAggregator with Amazon S3 features
 *
 * @author jungjan, zaizhang
 *
 */
public class S3SubmodelAggregator extends SubmodelAggregator {
	
	private AmazonS3 s3Client;
	private String bucketName;

	public S3SubmodelAggregator(ISubmodelAPIFactory smApiFactory, AmazonS3 s3Client, String bucketName) {
		super(smApiFactory);
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	@Override
	public void deleteSubmodelByIdentifier(IIdentifier identifier) {
		super.deleteSubmodelByIdentifier(identifier);
		deleteSubmodelFromDB(identifier.getId(), bucketName, s3Client);
	}

	@Override
	public void deleteSubmodelByIdShort(String idShort) {
		try {
			ISubmodel sm = getSubmodelbyIdShort(idShort);
			super.deleteSubmodelByIdShort(idShort);
			deleteSubmodelFromDB(sm.getIdentification().getId(), bucketName, s3Client);
		} catch (ResourceNotFoundException e) {
			// Nothing to do
		}
	}
	
	private void deleteSubmodelFromDB(String id, String bucketname, AmazonS3 s3Client) {
		if(S3Helper.isVersionedBucket(s3Client, bucketname)) {
			List<S3VersionSummary> summary = S3Helper.listBucketVersions(s3Client, bucketname)
													.getVersionSummaries()
													.stream()
													.filter(vs->vs.getKey().contains(id))
													.collect(Collectors.toList());
			summary.forEach(vs->{
				S3Helper.deleteObjectFromVersionedBucket(s3Client, bucketname, id, vs.getVersionId());
			});
			return;
		}
		S3Helper.deleteObjectFromUnversionedBucket(s3Client, bucketname, id);
	}
	
	public void reset() {
		smApiMap.clear();
		S3Helper.wipeVersionedBucket(s3Client, bucketName);
	}
}
