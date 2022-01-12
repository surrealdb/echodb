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
use futures::lock::Mutex;
use im::OrdMap;

pub struct Db<K, V> {
	pub(crate) lk: Mutex<()>,
	pub(crate) ds: OrdMap<K, V>,
}

// Open a new database
pub fn new<K, V>() -> Db<K, V> {
	Db {
		lk: Mutex::new(()),
		ds: OrdMap::new(),
	}
}

impl<K, V> Db<K, V>
where
	K: Ord + Clone,
	V: Clone,
{
	// Start a new transaction
	pub fn begin(&self, write: bool) -> Result<Tx<K, V>, Error> {
		match write {
			true => Ok(Tx::new(self, write, Some(self.lk.lock()))),
			false => Ok(Tx::new(self, write, None)),
		}
	}
}
