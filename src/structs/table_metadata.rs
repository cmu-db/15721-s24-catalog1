// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
//! The main struct here is [TableMetadataV2] which defines the data for a table.

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;


use _serde::TableMetadataEnum;


use chrono::{DateTime, TimeZone, Utc};

static MAIN_BRANCH: &str = "main";
static DEFAULT_SPEC_ID: i32 = 0;
static DEFAULT_SORT_ORDER_ID: i64 = 0;

pub(crate) static EMPTY_SNAPSHOT_ID: i64 = -1;
pub(crate) static INITIAL_SEQUENCE_NUMBER: i64 = 0;

/// Reference to [`TableMetadata`].
pub type TableMetadataRef = Arc<TableMetadata>;

#[derive(Debug, PartialEq, Deserialize, Eq, Clone)]
#[serde(try_from = "TableMetadataEnum")]
/// Fields for the version 2 of the table metadata.
///
/// We assume that this data structure is always valid, so we will panic when invalid error happens.
/// We check the validity of this data structure when constructing.
pub struct TableMetadata {
    /// Integer Version for the format.
    pub(crate) format_version: FormatVersion,
    /// A UUID that identifies the table
    pub(crate) table_uuid: Uuid,
    /// The tables highest sequence number
    pub(crate) last_sequence_number: i64,
    /// An integer; the highest assigned column ID for the table.
    pub(crate) last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub(crate) schemas: HashMap<i32, SchemaRef>,
    /// ID of the table’s current schema.
    pub(crate) current_schema_id: i32,
    /// ID of the “current” spec that writers should use by default.
    pub(crate) default_spec_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    pub(crate) properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Returns format version of this metadata.
    #[inline]
    pub fn format_version(&self) -> FormatVersion {
        self.format_version
    }

    /// Returns uuid of current table.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.table_uuid
    }
    /// Returns last sequence number.
    #[inline]
    pub fn last_sequence_number(&self) -> i64 {
        self.last_sequence_number
    }

    /// Returns schemas
    #[inline]
    pub fn schemas_iter(&self) -> impl Iterator<Item = &SchemaRef> {
        self.schemas.values()
    }

    /// Lookup schema by id.
    #[inline]
    pub fn schema_by_id(&self, schema_id: SchemaId) -> Option<&SchemaRef> {
        self.schemas.get(&schema_id)
    }

    /// Get current schema
    #[inline]
    pub fn current_schema(&self) -> &SchemaRef {
        self.schema_by_id(self.current_schema_id)
            .expect("Current schema id set, but not found in table metadata")
    }

    /// Returns properties of table.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Manipulating table metadata.
pub struct TableMetadataBuilder(TableMetadata);

impl TableMetadataBuilder {
    /// Creates a new table metadata builder from the given table metadata.
    pub fn new(origin: TableMetadata) -> Self {
        Self(origin)
    }

    /// Creates a new table metadata builder from the given table creation.
    pub fn from_table_creation(table_creation: TableCreation) -> Result<Self> {
        let TableCreation {
            name: _,
            location,
            schema,
            partition_spec,
            sort_order,
            properties,
        } = table_creation;

        let partition_specs = match partition_spec {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Can't create table with partition spec now",
                ))
            }
            None => HashMap::from([(
                DEFAULT_SPEC_ID,
                Arc::new(PartitionSpec {
                    spec_id: DEFAULT_SPEC_ID,
                    fields: vec![],
                }),
            )]),
        };

        let sort_orders = match sort_order {
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Can't create table with sort order now",
                ))
            }
            None => HashMap::from([(
                DEFAULT_SORT_ORDER_ID,
                Arc::new(SortOrder {
                    order_id: DEFAULT_SORT_ORDER_ID,
                    fields: vec![],
                }),
            )]),
        };

        let table_metadata = TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: Uuid::new_v4(),
            location: location.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Can't create table without location",
                )
            })?,
            last_sequence_number: 0,
            last_updated_ms: Utc::now().timestamp_millis(),
            last_column_id: schema.highest_field_id(),
            current_schema_id: schema.schema_id(),
            schemas: HashMap::from([(schema.schema_id(), Arc::new(schema))]),
            partition_specs,
            default_spec_id: DEFAULT_SPEC_ID,
            last_partition_id: 0,
            properties,
            current_snapshot_id: None,
            snapshots: Default::default(),
            snapshot_log: vec![],
            sort_orders,
            metadata_log: vec![],
            default_sort_order_id: DEFAULT_SORT_ORDER_ID,
            refs: Default::default(),
        };

        Ok(Self(table_metadata))
    }

    /// Changes uuid of table metadata.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Result<Self> {
        self.0.table_uuid = uuid;
        Ok(self)
    }

    /// Returns the new table metadata after changes.
    pub fn build(self) -> Result<TableMetadata> {
        Ok(self.0)
    }
}
