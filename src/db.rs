// Copyright © SurrealDB Ltd
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

//! This module stores the core in-memory database type.

use crate::tx::Transaction;
use arc_swap::ArcSwap;
use imbl::OrdMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A transactional in-memory database
#[derive(Clone)]
pub struct Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The datastore transaction write lock
	pub(crate) writelock: Arc<Mutex<()>>,
	/// The underlying copy-on-write B-tree datastructure
	pub(crate) datastore: Arc<ArcSwap<OrdMap<K, V>>>,
}

/// Create a new transactional in-memory database
pub fn new<K, V>() -> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	Database {
		datastore: Arc::new(ArcSwap::new(Arc::new(OrdMap::new()))),
		writelock: Arc::new(Mutex::new(())),
	}
}

impl<K, V> Database<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Start a new read-only or writeable transaction
	pub async fn begin(&self, write: bool) -> Transaction<K, V> {
		match write {
			true => {
				let lock = Some(self.writelock.clone().lock_owned().await);
				Transaction::write(self.clone(), lock)
			}
			false => {
				let lock = None;
				Transaction::read(self.clone(), lock)
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kv::{Key, Val};

	#[tokio::test]
	async fn begin_tx_readable() {
		let db: Database<Key, Val> = new();
		db.begin(false).await;
	}

	#[tokio::test]
	async fn begin_tx_writeable() {
		let db: Database<Key, Val> = new();
		db.begin(true).await;
	}

	#[tokio::test]
	async fn writeable_tx_async() {
		let db: Database<&str, &str> = new();
		let mut tx = db.begin(true).await;
		let res = async { tx.put("test", "something") }.await;
		assert!(res.is_ok());
		let res = async { tx.get("test") }.await;
		assert!(res.is_ok());
		let res = async { tx.commit() }.await;
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn readable_tx_not_writable() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false).await;
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn finished_tx_not_writeable() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false).await;
		let res = tx.cancel();
		assert!(res.is_ok());
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_err());
	}

	#[tokio::test]
	async fn cancelled_tx_is_cancelled() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await;
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false).await;
		let res = tx.exists("test").unwrap();
		assert_eq!(res, false);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn committed_tx_is_committed() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await;
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false).await;
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn multiple_concurrent_readers() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await;
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false).await;
		let res = tx1.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut tx2 = db.begin(false).await;
		let res = tx2.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn multiple_concurrent_operators() {
		let db: Database<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await;
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false).await;
		let res = tx1.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut txw = db.begin(true).await;
		txw.put("temp", "other").unwrap();
		let res = txw.exists("test").unwrap();
		assert_eq!(res, true);
		let res = txw.exists("temp").unwrap();
		assert_eq!(res, true);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.begin(false).await;
		let res = tx2.exists("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exists("temp").unwrap();
		assert_eq!(res, true);
		// ----------
		let res = tx1.exists("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}
}
