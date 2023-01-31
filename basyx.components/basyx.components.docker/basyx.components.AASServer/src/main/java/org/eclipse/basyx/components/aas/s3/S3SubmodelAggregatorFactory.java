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

import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregator;
import org.eclipse.basyx.submodel.aggregator.api.ISubmodelAggregatorFactory;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPIFactory;

import com.amazonaws.services.s3.AmazonS3;

/**
 * Implementation of the ISubmodelAggregatorFactory with Amazon S3 features
 * 
 * @author jungjan, zaizhang
 */

public class S3SubmodelAggregatorFactory implements ISubmodelAggregatorFactory {
	private AmazonS3 s3Client;
	private String bucketName;
	private ISubmodelAPIFactory smApiFactory;
	
	
	public S3SubmodelAggregatorFactory(AmazonS3 s3Client, String bucketName, ISubmodelAPIFactory submodelAPIFactory) {
		this.s3Client = s3Client;
		this.bucketName = bucketName;
		this.smApiFactory = submodelAPIFactory;
	}

	@Override
	public ISubmodelAggregator create() {
		return new S3SubmodelAggregator(smApiFactory, s3Client, bucketName);
	}
}
