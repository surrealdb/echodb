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

#![allow(clippy::bool_comparison)]

mod db;
mod err;
mod tx;

#[cfg(test)]
pub(crate) mod kv;

#[doc(inline)]
pub use self::db::*;
#[doc(inline)]
pub use self::err::*;
#[doc(inline)]
pub use self::tx::*;
