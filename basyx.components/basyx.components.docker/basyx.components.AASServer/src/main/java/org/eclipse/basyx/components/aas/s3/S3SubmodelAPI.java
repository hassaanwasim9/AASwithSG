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



import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.eclipse.basyx.submodel.metamodel.api.ISubmodel;
import org.eclipse.basyx.submodel.metamodel.api.submodelelement.ISubmodelElement;
import org.eclipse.basyx.submodel.metamodel.api.submodelelement.operation.IOperation;
import org.eclipse.basyx.submodel.metamodel.facade.submodelelement.SubmodelElementFacadeFactory;
import org.eclipse.basyx.submodel.metamodel.map.Submodel;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.SubmodelElement;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.SubmodelElementCollection;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.dataelement.property.Property;
import org.eclipse.basyx.submodel.metamodel.map.submodelelement.operation.Operation;
import org.eclipse.basyx.submodel.restapi.SubmodelElementProvider;
import org.eclipse.basyx.submodel.restapi.api.ISubmodelAPI;
import org.eclipse.basyx.submodel.restapi.operation.DelegatedInvocationManager;
import org.eclipse.basyx.vab.coder.json.serialization.DefaultTypeFactory;
import org.eclipse.basyx.vab.coder.json.serialization.GSONTools;
import org.eclipse.basyx.vab.exception.provider.MalformedRequestException;
import org.eclipse.basyx.vab.exception.provider.ResourceNotFoundException;
import org.eclipse.basyx.vab.modelprovider.VABPathTools;
import org.eclipse.basyx.vab.modelprovider.api.IModelProvider;
import org.eclipse.basyx.vab.modelprovider.map.VABMapProvider;
import org.eclipse.basyx.vab.protocol.http.connector.HTTPConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Implementation of the ISubmodelAPI with Amazon S3 features
 *
 * @author jungjan, zaizhang
 *
 */
public class S3SubmodelAPI implements ISubmodelAPI {
	private static Logger logger = LoggerFactory.getLogger(S3SubmodelAPI.class);

	private AmazonS3 s3Client;
	private String bucketName;
	private String submodelId;
	private GSONTools gsonTools = new GSONTools(new DefaultTypeFactory());
	private DelegatedInvocationManager invocationHelper;

	public S3SubmodelAPI(AmazonS3 s3Client, String bucketName, String submodelId) {
		this.s3Client = s3Client;
		this.bucketName = bucketName;
		this.submodelId = submodelId;
		this.invocationHelper = new DelegatedInvocationManager(new HTTPConnectorFactory());
	}

	public String getSubmodelId() {
		return submodelId;
	}

	public void setSubmodelId(String submodelId) {
		this.submodelId = submodelId;
	}

	public void setSubmodel(Submodel sm) throws IOException {
		String id = sm.getIdentification().getId();
		this.setSubmodelId(id);

		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ISubmodel getSubmodel() {
		String submodelJson = null;
		try {
			submodelJson = S3Helper.getBaSyxObjectContent(s3Client, bucketName, submodelId);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (submodelJson == null) {
			throw new ResourceNotFoundException("The submodel " + submodelId + " could not be found in the database.");
		}
		return Submodel.createAsFacade((Map<String, Object>) gsonTools.deserialize(submodelJson));
	}

	@Override
	public void addSubmodelElement(ISubmodelElement elem) {
		Submodel sm = (Submodel) getSubmodel();
		sm.addSubmodelElement(elem);

		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}

	@Override
	public void addSubmodelElement(String idShortPath, ISubmodelElement elem) {
		String[] splitted = VABPathTools.splitPath(idShortPath);
		List<String> idShorts = Arrays.asList(splitted);
		addNestedSubmodelElement(idShorts, elem);
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}
	
	private void addNestedSubmodelElement(List<String> idShorts, ISubmodelElement elem) {
		Submodel sm = (Submodel) getSubmodel();
		// > 1 idShorts => add new sm element to an existing sm element
		if (idShorts.size() > 1) {
			idShorts = idShorts.subList(0, idShorts.size() - 1);
			// Get parent SM element if more than 1 idShort
			ISubmodelElement parentElement = getNestedSubmodelElement(sm, idShorts);
			if (parentElement instanceof SubmodelElementCollection) {
				((SubmodelElementCollection) parentElement).addSubmodelElement(elem);
				// Replace db entry
				S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
			}
		} else {
			// else => directly add it to the submodel
			sm.addSubmodelElement(elem);
			// Replace db entry
			S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		}
	}


	@Override
	public ISubmodelElement getSubmodelElement(String idShortPath) {
		if (idShortPath.contains("/")) {
			String[] splitted = VABPathTools.splitPath(idShortPath);
			List<String> idShorts = Arrays.asList(splitted);
			return getNestedSubmodelElement(idShorts);
		} else {
			return getTopLevelSubmodelElement(idShortPath);
		}
	}

	private ISubmodelElement getTopLevelSubmodelElement(String idShort) {
		Submodel sm = (Submodel) getSubmodel();
		Map<String, ISubmodelElement> submodelElements = sm.getSubmodelElements();
		ISubmodelElement element = submodelElements.get(idShort);
		if (element == null) {
			throw new ResourceNotFoundException("The element \"" + idShort + "\" could not be found");
		}
		return convertSubmodelElement(element);
	}

	@SuppressWarnings("unchecked")
	private ISubmodelElement convertSubmodelElement(ISubmodelElement element) {
		// FIXME: Convert internal data structure of ISubmodelElement
		Map<String, Object> elementMap = (Map<String, Object>) element;
		IModelProvider elementProvider = new SubmodelElementProvider(new VABMapProvider(elementMap));
		Object elementVABObj = elementProvider.getValue("");
		return SubmodelElement.createAsFacade((Map<String, Object>) elementVABObj);
	}

	@SuppressWarnings("unchecked")
	private IModelProvider getElementProvider(Submodel sm, String idShortPath) {
		ISubmodelElement elem = sm.getSubmodelElement(idShortPath);
		IModelProvider mapProvider = new VABMapProvider((Map<String, Object>) elem);
		return SubmodelElementProvider.getElementProvider(mapProvider);
	}

	private ISubmodelElement getNestedSubmodelElement(Submodel sm, List<String> idShorts) {
		Map<String, ISubmodelElement> elemMap = sm.getSubmodelElements();
		// Get last nested submodel element
		for (int i = 0; i < idShorts.size() - 1; i++) {
			String idShort = idShorts.get(i);
			ISubmodelElement elem = elemMap.get(idShort);
			if (elem instanceof SubmodelElementCollection) {
				elemMap = ((SubmodelElementCollection) elem).getSubmodelElements();
			} else {
				throw new ResourceNotFoundException(
						idShort + " in the nested submodel element path could not be resolved.");
			}
		}
		String lastIdShort = idShorts.get(idShorts.size() - 1);
		if (!elemMap.containsKey(lastIdShort)) {
			throw new ResourceNotFoundException(
					lastIdShort + " in the nested submodel element path could not be resolved.");
		}
		return elemMap.get(lastIdShort);
	}

	private ISubmodelElement getNestedSubmodelElement(List<String> idShorts) {
		// Get sm from db
		Submodel sm = (Submodel) getSubmodel();
		// Get nested sm element from this sm
		return convertSubmodelElement(getNestedSubmodelElement(sm, idShorts));
	}

	@Override
	public void deleteSubmodelElement(String idShortPath) {
		if (idShortPath.contains("/")) {
			String[] splitted = VABPathTools.splitPath(idShortPath);
			List<String> idShorts = Arrays.asList(splitted);
			deleteNestedSubmodelElement(idShorts);
		} else {
			deleteTopLevelSubmodelElement(idShortPath);
		}

	}

	private void deleteNestedSubmodelElement(List<String> idShorts) {
		if (idShorts.size() == 1) {
			deleteSubmodelElement(idShorts.get(0));
			return;
		}

		// Get sm from db
		Submodel sm = (Submodel) getSubmodel();
		// Get parent collection
		List<String> parentIds = idShorts.subList(0, idShorts.size() - 1);
		ISubmodelElement parentElement = getNestedSubmodelElement(sm, parentIds);
		// Remove element
		SubmodelElementCollection coll = (SubmodelElementCollection) parentElement;
		coll.deleteSubmodelElement(idShorts.get(idShorts.size() - 1));
		// Replace db entry
		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}

	private void deleteTopLevelSubmodelElement(String idShort) {
		// Get sm from db
		Submodel sm = (Submodel) getSubmodel();
		// Remove element
		sm.getSubmodelElements().remove(idShort);
		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}

	@Override
	public Collection<IOperation> getOperations() {
		Submodel sm = (Submodel) getSubmodel();
		return sm.getOperations().values();
	}

	@Override
	public Collection<ISubmodelElement> getSubmodelElements() {
		Submodel sm = (Submodel) getSubmodel();
		return sm.getSubmodelElements().values();
	}

	@Override
	public void updateSubmodelElement(String idShortPath, Object newValue) {
		if (idShortPath.contains("/")) {
			String[] splitted = VABPathTools.splitPath(idShortPath);
			List<String> idShorts = Arrays.asList(splitted);
			updateNestedSubmodelElement(idShorts, newValue);
		} else {
			updateTopLevelSubmodelElement(idShortPath, newValue);
		}

	}

	@SuppressWarnings("unchecked")
	private void updateNestedSubmodelElement(List<String> idShorts, Object newValue) {
		Submodel sm = (Submodel) getSubmodel();

		// Get parent SM element
		ISubmodelElement element = getNestedSubmodelElement(sm, idShorts);

		// Update value
		IModelProvider mapProvider = new VABMapProvider((Map<String, Object>) element);
		IModelProvider elemProvider = SubmodelElementProvider.getElementProvider(mapProvider);
		elemProvider.setValue(Property.VALUE, newValue);

		// Replace db entry
		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}

	private void updateTopLevelSubmodelElement(String idShort, Object newValue) {
		// Get sm from db
		Submodel sm = (Submodel) getSubmodel();
		// Unwrap value
		newValue = unwrapParameter(newValue);
		// Get and update property value
		getElementProvider(sm, idShort).setValue(Property.VALUE, newValue);
		// Replace db entry
		S3Helper.uploadSubmodelToBucket(s3Client, bucketName, sm, getSubmodelIdentificationId());
		logger.info("Uploaded submodel to bucket '{}'. Method: {}.", bucketName, new Object() {
		}.getClass().getEnclosingMethod().getName());
	}

	@SuppressWarnings("unchecked")
	protected Object unwrapParameter(Object parameter) {
		if (parameter instanceof Map<?, ?>) {
			Map<String, Object> map = (Map<String, Object>) parameter;
			// Parameters have a strictly defined order and may not be omitted at all.
			// Enforcing the structure with valueType is ok, but we should unwrap null
			// values, too.
			if (map.get("valueType") != null && map.containsKey("value")) {
				return map.get("value");
			}
		}
		return parameter;
	}

	@Override
	public Object getSubmodelElementValue(String idShortPath) {
		if (idShortPath.contains("/")) {
			String[] splitted = VABPathTools.splitPath(idShortPath);
			List<String> idShorts = Arrays.asList(splitted);
			return getNestedSubmodelElementValue(idShorts);
		} else {
			return getTopLevelSubmodelElementValue(idShortPath);
		}
	}

	@SuppressWarnings("unchecked")
	private Object getNestedSubmodelElementValue(List<String> idShorts) {
		ISubmodelElement lastElement = getNestedSubmodelElement(idShorts);
		IModelProvider mapProvider = new VABMapProvider((Map<String, Object>) lastElement);
		return SubmodelElementProvider.getElementProvider(mapProvider).getValue("/value");
	}

	private Object getTopLevelSubmodelElementValue(String idShort) {
		Submodel sm = (Submodel) getSubmodel();
		return getElementProvider(sm, idShort).getValue("/value");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invokeOperation(String idShortPath, Object... params) {
		String elementPath = VABPathTools.getParentPath(idShortPath);

		Operation operation = (Operation) SubmodelElementFacadeFactory
				.createSubmodelElement((Map<String, Object>) getSubmodelElement(elementPath));
		if (!DelegatedInvocationManager.isDelegatingOperation(operation)) {
			throw new MalformedRequestException("This backend supports only delegating operations.");
		}
		return invocationHelper.invokeDelegatedOperation(operation, params);
	}

	@Override
	public Object invokeAsync(String idShortPath, Object... params) {
		String[] splitted = VABPathTools.splitPath(idShortPath);
		List<String> idShorts = Arrays.asList(splitted);
		return invokeNestedOperationAsync(idShorts, params);
	}

	private Object invokeNestedOperationAsync(List<String> idShorts, Object... params) {
		// not possible to invoke operations on a submodel that is stored in a db
		throw new MalformedRequestException("Invoke not supported by this backend");
	}

	@Override
	public Object getOperationResult(String idShort, String requestId) {
		// not possible to invoke operations on a submodel that is stored in a db
		throw new MalformedRequestException("Invoke not supported by this backend");
	}

	private String getSubmodelIdentificationId() {
		return getSubmodel().getIdentification().getId();
	}

	public String getSubmodelObject(AmazonS3 s3Client, String bucketName, String submodelId) throws IOException {
		S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, submodelId));
		String submodelJson = fullObject.getObjectContent().toString();
		fullObject.close();
		return submodelJson;
	}
}
