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

use crate::err::Error;
use crate::tx::Tx;
use arc_swap::ArcSwap;
use imbl::OrdMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Db<K, V> {
	pub(crate) lk: Arc<Mutex<()>>,
	pub(crate) ds: Arc<ArcSwap<OrdMap<K, V>>>,
}

// Open a new database
pub fn new<K, V>() -> Db<K, V> {
	Db {
		lk: Arc::new(Mutex::new(())),
		ds: Arc::new(ArcSwap::new(Arc::new(OrdMap::new()))),
	}
}

impl<K, V> Db<K, V>
where
	K: Ord + Clone,
	V: Eq + Clone,
{
	// Start a new transaction
	pub async fn begin(&self, write: bool) -> Result<Tx<K, V>, Error> {
		match write {
			true => Ok(Tx::new(self.ds.clone(), write, Some(self.lk.clone().lock_owned().await))),
			false => Ok(Tx::new(self.ds.clone(), write, None)),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kv::{Key, Val};

	#[tokio::test]
	async fn begin_tx_readable() {
		let db: Db<Key, Val> = new();
		let tx = db.begin(false).await;
		assert!(tx.is_ok());
	}

	#[tokio::test]
	async fn begin_tx_writeable() {
		let db: Db<Key, Val> = new();
		let tx = db.begin(true).await;
		assert!(tx.is_ok());
	}

	#[tokio::test]
	async fn begin_tx_readable_async() {
		let db: Db<Key, Val> = new();
		let tx = async { db.begin(false).await };
		assert!(tx.await.is_ok());
	}

	#[tokio::test]
	async fn begin_tx_writeable_async() {
		let db: Db<Key, Val> = new();
		let tx = async { db.begin(true).await };
		assert!(tx.await.is_ok());
	}

	#[tokio::test]
	async fn writeable_tx_across_async() {
		let db: Db<&str, &str> = new();
		let mut tx = db.begin(true).await.unwrap();
		let res = async { tx.put("test", "something") }.await;
		assert!(res.is_ok());
		let res = async { tx.get("test") }.await;
		assert!(res.is_ok());
		let res = async { tx.commit() }.await;
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn readable_tx_not_writable() {
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false).await.unwrap();
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
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(false).await.unwrap();
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
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await.unwrap();
		tx.put("test", "something").unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false).await.unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, false);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn committed_tx_is_committed() {
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await.unwrap();
		tx.put("test", "something").unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.begin(false).await.unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn multiple_concurrent_readers() {
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await.unwrap();
		tx.put("test", "something").unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false).await.unwrap();
		let res = tx1.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exi("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut tx2 = db.begin(false).await.unwrap();
		let res = tx2.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exi("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[tokio::test]
	async fn multiple_concurrent_operators() {
		let db: Db<&str, &str> = new();
		// ----------
		let mut tx = db.begin(true).await.unwrap();
		tx.put("test", "something").unwrap();
		let res = tx.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx.get("test").unwrap();
		assert_eq!(res, Some("something"));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.begin(false).await.unwrap();
		let res = tx1.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx1.exi("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let mut txw = db.begin(true).await.unwrap();
		txw.put("temp", "other").unwrap();
		let res = txw.exi("test").unwrap();
		assert_eq!(res, true);
		let res = txw.exi("temp").unwrap();
		assert_eq!(res, true);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.begin(false).await.unwrap();
		let res = tx2.exi("test").unwrap();
		assert_eq!(res, true);
		let res = tx2.exi("temp").unwrap();
		assert_eq!(res, true);
		// ----------
		let res = tx1.exi("temp").unwrap();
		assert_eq!(res, false);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}
}
