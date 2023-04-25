use std::collections::HashMap;
use std::fs;

use itertools::Itertools;
use j4rs::{ClasspathEntry, InvocationArg, JavaClass, Jvm, JvmBuilder};
use risingwave_pb::connector_service::{SourceType, TableSchema};

pub struct JvmWrapper {
    jvm: Jvm,
}

unsafe impl Send for JvmWrapper {}
unsafe impl Sync for JvmWrapper {}

impl JvmWrapper {
    pub fn create_jvm() -> anyhow::Result<Self> {
        // TODO(j4rs): fix class path
        let jars = fs::read_dir(".risingwave/bin/connector-node/libs").unwrap();
        let jar_paths = jars
            .into_iter()
            .map(|jar| jar.unwrap().path().display().to_string())
            .collect_vec();
        let classpath_entries = jar_paths
            .iter()
            .map(|p| {
                println!("adding {} to class path", p.as_str());
                ClasspathEntry::new(p.as_str())
            })
            .collect_vec();
        let jvm = JvmBuilder::new()
            .classpath_entries(classpath_entries)
            .build()
            .map_err(|e| anyhow::format_err!("cannot create jvm: {}", e.to_string()))?;
        Ok(JvmWrapper { jvm })
    }

    pub fn validate_source_properties(
        &self,
        source_type: SourceType,
        properties: HashMap<String, String>,
        table_schema: TableSchema,
    ) {
        let properties_java = self
            .jvm
            .java_map(JavaClass::String, JavaClass::String, properties)
            .unwrap();
        self.jvm
            .invoke_static(
                "com.risingwave.connector.SourceHandlerIpc",
                "handleValidate",
                vec![
                    InvocationArg::new(&source_type, "com.risingwave.sourcenode.types.SourceType"),
                    InvocationArg::try_from(properties_java).unwrap(),
                    InvocationArg::new(
                        &table_schema,
                        "com.risingwave.sourcenode.types.TableSchema",
                    ),
                ]
                .as_slice(),
            )
            .unwrap();
    }
}
