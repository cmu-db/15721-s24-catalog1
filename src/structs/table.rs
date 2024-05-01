use crate::table::Table;
use crate::structs::error::{Error, ErrorKind, Result};
use crate::structs::namespace::{Namespace, NamespaceIdent};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::take;
use std::ops::Deref;
use typed_builder::TypedBuilder;
use urlencoding::encode;
use uuid::Uuid;


/// TableIdent represents the identifier of a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Namespace of the table.
    pub namespace: NamespaceIdent,
    /// Table name.
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier.
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }

    /// Get the namespace of the table.
    /// this returns the identifier of the namespace
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Try to create table identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let mut vec: Vec<String> = iter.into_iter().map(|s| s.to_string()).collect();
        let table_name = vec.pop().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Table identifier can't be empty!")
        })?;
        let namespace_ident = NamespaceIdent::from_vec(vec)?;

        Ok(Self {
            namespace: namespace_ident,
            name: table_name,
        })
    }
}

/// TableCreation represents the creation of a table in the catalog.
#[derive(Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    /// The location of the table.
    #[builder(default, setter(strip_option))]
    pub location: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
    /// The partition spec of the table, could be None.
    #[builder(default, setter(strip_option))]
    pub partition_spec: Option<UnboundPartitionSpec>,
    /// The sort order of the table.
    #[builder(default, setter(strip_option))]
    pub sort_order: Option<SortOrder>,
    /// The properties of the table.
    #[builder(default)]
    pub properties: HashMap<String, String>,
}

/// TableCommit represents the commit of a table in the catalog.
#[derive(Debug, TypedBuilder)]
#[builder(build_method(vis = "pub(crate)"))]
pub struct TableCommit {
    /// The table ident.
    ident: TableIdent,
    /// The requirements of the table.
    ///
    /// Commit will fail if the requirements are not met.
    requirements: Vec<TableRequirement>,
    /// The updates of the table.
    updates: Vec<TableUpdate>,
}

impl TableCommit {
    /// Return the table identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.ident
    }

    /// Take all requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }

    /// Take all updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }
}

/// TableRequirement represents a requirement for a table in the catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TableRequirement {
    /// The table must not already exist; used for create transactions
    #[serde(rename = "assert-create")]
    NotExist,
    /// The table UUID must match the requirement.
    #[serde(rename = "assert-table-uuid")]
    UuidMatch {
        /// Uuid of original table.
        uuid: Uuid,
    },
    /// The table branch or tag identified by the requirement's `reference` must
    /// reference the requirement's `snapshot-id`.
    #[serde(rename = "assert-ref-snapshot-id")]
    RefSnapshotIdMatch {
        /// The reference of the table to assert.
        r#ref: String,
        /// The snapshot id of the table to assert.
        /// If the id is `None`, the ref must not already exist.
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    /// The table's last assigned column id must match the requirement.
    #[serde(rename = "assert-last-assigned-field-id")]
    LastAssignedFieldIdMatch {
        /// The last assigned field id of the table to assert.
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i64,
    },
    /// The table's current schema id must match the requirement.
    #[serde(rename = "assert-current-schema-id")]
    CurrentSchemaIdMatch {
        /// Current schema id of the table to assert.
        #[serde(rename = "current-schema-id")]
        current_schema_id: i64,
    },
    /// The table's last assigned partition id must match the
    /// requirement.
    #[serde(rename = "assert-last-assigned-partition-id")]
    LastAssignedPartitionIdMatch {
        /// Last assigned partition id of the table to assert.
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i64,
    },
    /// The table's default spec id must match the requirement.
    #[serde(rename = "assert-default-spec-id")]
    DefaultSpecIdMatch {
        /// Default spec id of the table to assert.
        #[serde(rename = "default-spec-id")]
        default_spec_id: i64,
    },
    /// The table's default sort order id must match the requirement.
    #[serde(rename = "assert-default-sort-order-id")]
    DefaultSortOrderIdMatch {
        /// Default sort order id of the table to assert.
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i64,
    },
}

/// TableUpdate represents an update to a table in the catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Upgrade table's format version
    #[serde(rename_all = "kebab-case")]
    UpgradeFormatVersion {
        /// Target format upgrade to.
        format_version: FormatVersion,
    },
    /// Assign a new UUID to the table
    #[serde(rename_all = "kebab-case")]
    AssignUuid {
        /// The new UUID to assign.
        uuid: Uuid,
    },
    /// Add a new schema to the table
    #[serde(rename_all = "kebab-case")]
    AddSchema {
        /// The schema to add.
        schema: Schema,
        /// The last column id of the table.
        last_column_id: Option<i32>,
    },
    /// Set table's current schema
    #[serde(rename_all = "kebab-case")]
    SetCurrentSchema {
        /// Schema ID to set as current, or -1 to set last added schema
        schema_id: i32,
    },
    /// Add a new partition spec to the table
    AddSpec {
        /// The partition spec to add.
        spec: UnboundPartitionSpec,
    },
    /// Set table's default spec
    #[serde(rename_all = "kebab-case")]
    SetDefaultSpec {
        /// Partition spec ID to set as the default, or -1 to set last added spec
        spec_id: i32,
    },
    /// Add sort order to table.
    #[serde(rename_all = "kebab-case")]
    AddSortOrder {
        /// Sort order to add.
        sort_order: SortOrder,
    },
    /// Set table's default sort order
    #[serde(rename_all = "kebab-case")]
    SetDefaultSortOrder {
        /// Sort order ID to set as the default, or -1 to set last added sort order
        sort_order_id: i32,
    },
    /// Add snapshot to table.
    #[serde(rename_all = "kebab-case")]
    AddSnapshot {
        /// Snapshot to add.
        snapshot: Snapshot,
    },
    /// Set table's snapshot ref.
    #[serde(rename_all = "kebab-case")]
    SetSnapshotRef {
        /// Name of snapshot reference to set.
        ref_name: String,
        /// Snapshot reference to set.
        #[serde(flatten)]
        reference: SnapshotReference,
    },
    /// Remove table's snapshots
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshots {
        /// Snapshot ids to remove.
        snapshot_ids: Vec<i64>,
    },
    /// Remove snapshot reference
    #[serde(rename_all = "kebab-case")]
    RemoveSnapshotRef {
        /// Name of snapshot reference to remove.
        ref_name: String,
    },
    /// Update table's location
    SetLocation {
        /// New location for table.
        location: String,
    },
    /// Update table's properties
    SetProperties {
        /// Properties to update for table.
        updates: HashMap<String, String>,
    },
    /// Remove table's properties
    RemoveProperties {
        /// Properties to remove
        removals: Vec<String>,
    },
}

impl TableUpdate {
    /// Applies the update to the table metadata builder.
    pub fn apply(self, builder: TableMetadataBuilder) -> Result<TableMetadataBuilder> {
        match self {
            TableUpdate::AssignUuid { uuid } => builder.assign_uuid(uuid),
            _ => unimplemented!(),
        }
    }
}
