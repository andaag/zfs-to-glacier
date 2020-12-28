use crate::cmd_execute::*;
use chrono::prelude::*;
use std::fmt;
use std::str;
use std::{collections::HashMap, error::Error};

#[derive(Hash, Clone, Eq, PartialEq, Debug)]
pub struct ZfsSnapshot {
    pub name: String,
    pub creation: DateTime<Local>,
}

impl fmt::Display for ZfsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZfsSnapshot {}",
            self.name,
            //self.creation.format("%Y-%m-%d %H:%M:%S")
        )
    }
}

pub struct LocalZfsState {
    pub pools: HashMap<String, Vec<ZfsSnapshot>>,
}

pub fn get_local_zfs_state() -> Result<LocalZfsState, Box<dyn Error>> {
    let pools = { ExecutorCommand("zfs list -Hp -o name".to_string()).execute_by_line() }?;

    let snapshots = {
        ExecutorCommand("zfs list -Hpt snapshot -o name,creation -s creation".to_string())
            .execute_by_line()
            .map(|lines| {
                lines
                    .iter()
                    .map(|x| {
                        let s: Vec<&str> = x.split("\t").collect();
                        ZfsSnapshot {
                            name: s[0].to_string(),
                            creation: Local.timestamp(s[1].parse::<i64>().unwrap(), 0),
                        }
                    })
                    .collect::<Vec<ZfsSnapshot>>()
            })
    }?;

    let mut result: HashMap<String, Vec<ZfsSnapshot>> = HashMap::new();
    for pool in pools {
        let mut pool_start = pool.to_owned();
        pool_start.push_str("@");
        let snapshots_for_pool: Vec<ZfsSnapshot> = snapshots
            .iter()
            .filter(|x| x.name.starts_with(&pool_start))
            .map(|x| x.to_owned())
            .collect();
        result.insert(pool, snapshots_for_pool);
    }
    Ok(LocalZfsState { pools: result })
}
