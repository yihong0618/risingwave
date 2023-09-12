// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ```cargo
//! [package]
//! edition = "2021"
//!
//! [dependencies]
//! toml = "0.4"
//! serde = { version = "1", features = ["derive"] }
//! tokio = { version = "1", features = ["full"] }
//! tokio-postgres = "0.7"
//! ```

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use serde::Serialize;
use tokio_postgres::{Error, NoTls};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    {
        // Lock the server version for reproducibility.
        let rows = client.query("SHOW server_version_num;", &[]).await?;
        let value: &str = rows[0].get(0);
        assert_eq!(value, "150003", "Expect Postgres 15.3 for reproducibility");
    }

    {
        #[derive(Debug, Serialize)]
        struct PgType {
            oid: i32,
            typtype: i8,
            typarray: u32,
            typname: String,
            typlen: i16,
        }
        #[derive(Debug, Serialize)]
        struct Table {
            types: Vec<PgType>,
        }
        let sql = "\
        select oid, typtype, typarray, typname, typlen \
        from pg_type \
        where typtype = 'b' and typnamespace = ( \
            select oid from pg_namespace where nspname = 'pg_catalog'\
        ) and \
        not starts_with(typname, '_');
        ";
        let rows = client.query(sql, &[]).await?;
        let mut types = vec![];
        for row in rows {
            let oid: u32 = row.get(0);
            let typtype: i8 = row.get(1);
            let typarray: u32 = row.get(2);
            let typname: &str = row.get(3);
            let typlen: i16 = row.get(4);
            let pg_type = PgType {
                oid: oid as i32,
                typtype,
                typarray,
                typname: typname.to_string(),
                typlen,
            };
            types.push(pg_type);
        }
        let encoded_table = toml::to_string(&Table { types }).unwrap();
        let this_file = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut file = File::create(this_file.join("pg_type.toml")).unwrap();
        file.write_all(encoded_table.as_bytes()).unwrap();
    }

    Ok(())
}
