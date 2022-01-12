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

use crate::db::Db;
use crate::err::Error;
use crate::ptr::Ptr;
use futures::lock::MutexLockFuture;
use im::OrdMap;
use std::cell::RefCell;
use std::ops::Range;
use std::pin::Pin;

pub struct Tx<'a, K, V> {
	// Is the transaction complete?
	pub(crate) ok: bool,
	// Is the transaction read+write?
	pub(crate) rw: bool,
	// The immutable copy of the data map
	pub(crate) ds: OrdMap<K, V>,
	// The underlying database write mutex
	pub(crate) lk: Option<MutexLockFuture<'a, ()>>,
	// The back reference to the parent database
	pub(crate) db: RefCell<Pin<Box<Ptr<Db<K, V>>>>>,
}

impl<'a, K, V> Tx<'a, K, V>
where
	K: Ord + Clone,
	V: Clone,
{
	// Create a transaction
	pub(crate) fn new(
		db: &'a Db<K, V>,
		write: bool,
		guard: Option<MutexLockFuture<'a, ()>>,
	) -> Tx<'a, K, V> {
		Tx {
			ok: false,
			rw: write,
			lk: guard,
			ds: db.ds.clone(),
			db: RefCell::new(Pin::new(Box::new(Ptr::new(db)))),
		}
	}
	// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	// Cancel a transaction
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.ok = true;
		// Unlock the database mutex
		if let Some(lk) = &self.lk.take() {
			drop(lk);
		}
		// Continue
		Ok(())
	}
	// Commit a transaction
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.ok = true;
		// Commit the data
		let mut db = self.db.borrow_mut();
		let _ = std::mem::replace(&mut db.ds, self.ds.clone());
		// Unlock the database mutex
		if let Some(lk) = &self.lk.take() {
			drop(lk);
		}
		// Continue
		Ok(())
	}
	// Delete a key
	pub fn del(&mut self, key: K) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.ds.remove(&key);
		// Return result
		Ok(())
	}
	// Check if a key exists
	pub fn exi(&self, key: K) -> Result<bool, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = self.ds.contains_key(&key);
		// Return result
		Ok(res)
	}
	// Fetch a key from the database
	pub fn get(&self, key: K) -> Result<Option<V>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = self.ds.get(&key).cloned();
		// Return result
		Ok(res)
	}
	// Insert or update a key in the database
	pub fn set(&mut self, key: K, val: V) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.ds.insert(key, val);
		// Return result
		Ok(())
	}
	// Insert a key if it doesn't exist in the database
	pub fn put(&mut self, key: K, val: V) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.rw {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.ds.insert(key, val);
		// Return result
		Ok(())
	}
	// Retrieve a range of keys from the databases
	pub fn scan(&self, rng: Range<K>, limit: u32) -> Result<Vec<(K, V)>, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxClosed);
		}
		// Scan the keys
		let res = self.ds.range(rng);
		let res = res.take(limit as usize).map(|(k, v)| (k.clone(), v.clone())).collect();
		// Return result
		Ok(res)
	}
}
