use std::collections::HashMap;
use std::fs;

use itertools::Itertools;
use j4rs::{ClasspathEntry, Instance, InvocationArg, JavaClass, Jvm, JvmBuilder};
use risingwave_pb::connector_service::{GetEventStreamResponse, SourceType, TableSchema};

pub struct JvmWrapper {
    pub inner: Jvm,
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
        Ok(JvmWrapper { inner: jvm })
    }

    pub fn validate_source_properties(
        &self,
        source_type: SourceType,
        properties: HashMap<String, String>,
        table_schema: TableSchema,
    ) {
        let properties_java = self
            .inner
            .java_map(JavaClass::String, JavaClass::String, properties)
            .unwrap();
        // TODO(j4rs): handle error correctly
        self.inner
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

    pub fn get_source_stream_handler(
        &self,
        source_id: u64,
        source_type: SourceType,
        start_offset: String,
        properties: HashMap<String, String>,
    ) -> Instance {
        let properties_java = self
            .inner
            .java_map(JavaClass::String, JavaClass::String, properties)
            .unwrap();
        // TODO(j4rs): handle error correctly
        return self
            .inner
            .invoke_static(
                "com.risingwave.connector.SourceHandlerIpc",
                "handleStart",
                vec![
                    InvocationArg::new(&source_id, "java.lang.Long"),
                    InvocationArg::new(&source_type, "com.risingwave.sourcenode.types.SourceType"),
                    InvocationArg::try_from(start_offset).unwrap(),
                    InvocationArg::try_from(properties_java).unwrap(),
                ]
                .as_slice(),
            )
            .unwrap();
    }

    pub fn start_source(&self, dbz_handler: &Instance) {
        // TODO(j4rs): handle error correctly
        self.inner
            .invoke(dbz_handler, "startSource", vec![].as_slice())
            .unwrap();
    }

    pub fn get_cdc_chunk(&self, handler_id: i64) -> GetEventStreamResponse {
        let res_java = self
            .inner
            .invoke_static(
                "com.risingwave.sourcenode.core.SourceHandlerManager",
                "getChunk",
                vec![InvocationArg::new(&handler_id, "java.lang.Long")].as_slice(),
            )
            .unwrap();
        // TODO(j4rs): handle error correctly
        self.inner.to_rust(res_java).unwrap()
    }
}
