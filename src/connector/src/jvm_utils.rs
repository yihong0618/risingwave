use std::fs;

use itertools::Itertools;
use j4rs::{ClasspathEntry, Jvm, JvmBuilder};

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
}
