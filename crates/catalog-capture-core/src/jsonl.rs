// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result};
use serde::Serialize;

pub fn append_jsonl_records<T: Serialize>(path: &Path, records: &[T], label: &str) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create {label} metadata dir {}", parent.display())
        })?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {label} metadata {}", path.display()))?;

    for record in records {
        serde_json::to_writer(&mut file, record)
            .with_context(|| format!("failed to serialize {label} record"))?;
        file.write_all(b"\n")
            .with_context(|| format!("failed to append {label} metadata {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SampleRecord {
        id: u32,
        name: String,
    }

    #[test]
    fn append_jsonl_records_writes_one_line_per_record() {
        let temp = std::env::temp_dir().join(format!("jsonl-append-{}", std::process::id()));
        fs::create_dir_all(&temp).unwrap();
        let path = temp.join("metadata/sample.jsonl");

        let records = [
            SampleRecord {
                id: 1,
                name: "alpha".to_string(),
            },
            SampleRecord {
                id: 2,
                name: "beta".to_string(),
            },
        ];
        append_jsonl_records(&path, &records, "sample").unwrap();

        let contents = fs::read_to_string(&path).expect("jsonl file");
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            serde_json::from_str::<SampleRecord>(lines[0]).unwrap(),
            records[0]
        );
        assert_eq!(
            serde_json::from_str::<SampleRecord>(lines[1]).unwrap(),
            records[1]
        );

        fs::remove_dir_all(temp).ok();
    }

    #[test]
    fn append_jsonl_records_no_ops_on_empty_input() {
        let temp = std::env::temp_dir().join(format!("jsonl-empty-{}", std::process::id()));
        fs::create_dir_all(&temp).unwrap();
        let path = temp.join("metadata/sample.jsonl");

        append_jsonl_records::<SampleRecord>(&path, &[], "sample").unwrap();
        assert!(!path.exists());

        fs::remove_dir_all(temp).ok();
    }
}
