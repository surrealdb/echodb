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

//! This module stores the database transaction logic.

use crate::err::Error;
use crate::Database;
use imbl::ordmap::Entry;
use imbl::OrdMap;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::mem::drop;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

/// A serializable snapshot isolated database transaction
pub struct Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// The current snapshot for this transaction
	snapshot: OrdMap<K, V>,
	/// The parent database for this transaction
	database: Database<K, V>,
	/// The parent datastore transaction write lock
	writelock: Option<OwnedMutexGuard<()>>,
}

impl<K, V> Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new read-only transaction
	pub(crate) fn read(db: Database<K, V>, lock: Option<OwnedMutexGuard<()>>) -> Transaction<K, V> {
		Transaction {
			done: false,
			write: false,
			snapshot: (*(*db.datastore.load())).clone(),
			database: db,
			writelock: lock,
		}
	}
	/// Create a new writeable transaction
	pub(crate) fn write(
		db: Database<K, V>,
		lock: Option<OwnedMutexGuard<()>>,
	) -> Transaction<K, V> {
		Transaction {
			done: false,
			write: true,
			snapshot: (*(*db.datastore.load())).clone(),
			database: db,
			writelock: lock,
		}
	}

	/// Check if the transaction is closed
	pub fn closed(&self) -> bool {
		self.done
	}

	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Release the commit lock
		if let Some(lock) = self.writelock.take() {
			drop(lock);
		}
		// Continue
		Ok(())
	}

	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Mark this transaction as done
		self.done = true;
		// Atomically update the datastore using ArcSwap
		self.database.datastore.store(Arc::new(self.snapshot.clone()));
		// Release the commit lock
		if let Some(lock) = self.writelock.take() {
			drop(lock);
		}
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub fn exists<Q>(&self, key: Q) -> Result<bool, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the key
		let res = self.snapshot.contains_key(key.borrow());
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub fn get<Q>(&self, key: Q) -> Result<Option<V>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Get the key
		let res = self.snapshot.get(key.borrow()).cloned();
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	pub fn set<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.snapshot.insert(key.into(), val);
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub fn put<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.snapshot.entry(key.into()) {
			Entry::Vacant(v) => {
				v.insert(val);
			}
			_ => return Err(Error::KeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if it matches a value
	pub fn putc<Q>(&mut self, key: Q, val: V, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match (self.snapshot.entry(key.into()), &chk) {
			(Entry::Occupied(mut v), Some(w)) if v.get() == w => {
				v.insert(val);
			}
			(Entry::Vacant(v), None) => {
				v.insert(val);
			}
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database
	pub fn del<Q>(&mut self, key: Q) -> Result<(), Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.snapshot.remove(key.borrow());
		// Return result
		Ok(())
	}

	/// Delete a key if it matches a value
	pub fn delc<Q>(&mut self, key: Q, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match (self.snapshot.entry(key.into()), &chk) {
			(Entry::Occupied(v), Some(w)) if v.get() == w => {
				v.remove();
			}
			(Entry::Vacant(_), None) => {
				// Nothing to delete
			}
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	pub fn keys<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<K>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Scan the keys
		let res = self.snapshot.range(beg..end).take(limit).map(|(k, _)| k.clone()).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs from the databases
	pub fn scan<Q>(&self, rng: Range<Q>, limit: usize) -> Result<Vec<(K, V)>, Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Compute the range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Scan the keys
		let res = self
			.snapshot
			.range(beg..end)
			.take(limit)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		// Return result
		Ok(res)
	}
}
