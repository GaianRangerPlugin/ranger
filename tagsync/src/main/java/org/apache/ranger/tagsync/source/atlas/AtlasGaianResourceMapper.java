/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.atlas;

import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.HashMap;
import java.util.Map;

public class AtlasGaianResourceMapper extends AtlasResourceMapper {
	public static final String ENTITY_TYPE_GAIAN_SCHEMA     = "gaian_schema";
	public static final String ENTITY_TYPE_GAIAN_TABLE  = "gaian_table";
	public static final String ENTITY_TYPE_GAIAN_COLUMN = "gaian_column";

	public static final String RANGER_TYPE_GAIAN_SCHEMA     = "schema";
	public static final String RANGER_TYPE_GAIAN_TABLE  = "table";
	public static final String RANGER_TYPE_GAIAN_COLUMN = "column";

	public static final String ENTITY_ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
	public static final String QUALIFIED_NAME_DELIMITER        = "\\.";

	public static final String[] SUPPORTED_ENTITY_TYPES = { ENTITY_TYPE_GAIAN_SCHEMA, ENTITY_TYPE_GAIAN_TABLE, ENTITY_TYPE_GAIAN_COLUMN };

	public AtlasGaianResourceMapper() {
		super("hive", SUPPORTED_ENTITY_TYPES);
	}

	@Override
	public RangerServiceResource buildResource(final IReferenceableInstance entity) throws Exception {
		String qualifiedName = getEntityAttribute(entity, ENTITY_ATTRIBUTE_QUALIFIED_NAME, String.class);
		if (StringUtils.isEmpty(qualifiedName)) {
			throw new Exception("attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "' not found in entity");
		}

		String resourceStr = getResourceNameFromQualifiedName(qualifiedName);
		if (StringUtils.isEmpty(resourceStr)) {
			throwExceptionWithMessage("resource not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
		}

		String clusterName = getClusterNameFromQualifiedName(qualifiedName);
		if (StringUtils.isEmpty(clusterName)) {
			throwExceptionWithMessage("cluster-name not found in attribute '" +  ENTITY_ATTRIBUTE_QUALIFIED_NAME + "': " + qualifiedName);
		}

		String   entityType  = entity.getTypeName();
		String   entityGuid  = entity.getId() != null ? entity.getId()._getId() : null;
		String   serviceName = getRangerServiceName(clusterName);
		String[] resources   = resourceStr.split(QUALIFIED_NAME_DELIMITER);
		String   schemaName      = resources.length > 0 ? resources[0] : null;
		String   tblName     = resources.length > 1 ? resources[1] : null;
		String   colName     = resources.length > 2 ? resources[2] : null;

		Map<String, RangerPolicyResource> elements = new HashMap<String, RangerPolicyResource>();

		if (StringUtils.equals(entityType, ENTITY_TYPE_GAIAN_SCHEMA)) {
			if (StringUtils.isNotEmpty(schemaName)) {
				elements.put(RANGER_TYPE_GAIAN_SCHEMA, new RangerPolicyResource(schemaName));
			}
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_GAIAN_TABLE)) {
			if (StringUtils.isNotEmpty(schemaName) && StringUtils.isNotEmpty(tblName)) {
				elements.put(RANGER_TYPE_GAIAN_SCHEMA, new RangerPolicyResource(schemaName));
				elements.put(RANGER_TYPE_GAIAN_TABLE, new RangerPolicyResource(tblName));
			}
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_GAIAN_COLUMN)) {
			if (StringUtils.isNotEmpty(schemaName) && StringUtils.isNotEmpty(tblName) && StringUtils.isNotEmpty(colName)) {
				elements.put(RANGER_TYPE_GAIAN_SCHEMA, new RangerPolicyResource(schemaName));
				elements.put(RANGER_TYPE_GAIAN_TABLE, new RangerPolicyResource(tblName));
				elements.put(RANGER_TYPE_GAIAN_COLUMN, new RangerPolicyResource(colName));
			}
		} else {
			throwExceptionWithMessage("unrecognized entity-type: " + entityType);
		}

		if(elements.isEmpty()) {
			throwExceptionWithMessage("invalid qualifiedName for entity-type '" + entityType + "': " + qualifiedName);
		}

		RangerServiceResource ret = new RangerServiceResource(entityGuid, serviceName, elements);

		return ret;
	}
}
