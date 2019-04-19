// Sonic
//
// Fast, lightweight and schema-less search backend
// Copyright: 2019, Valerian Saliou <valerian@valeriansaliou.name>
// License: Mozilla Public License v2.0 (MPL v2.0)

use crate::store::fst::StoreFSTActionBuilder;
use crate::store::item::StoreItem;
use crate::store::kv::{StoreKVAcquireMode, StoreKVActionBuilder, StoreKVPool};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

pub struct ExecutorFlushB;

impl ExecutorFlushB {
    pub fn execute(store: StoreItem) -> Result<u32, ()> {
        if let StoreItem(collection, Some(bucket), None) = store {
            // Important: acquire database access read lock, and reference it in context. This \
            //   prevents the database from being erased while using it in this block.
            general_kv_access_lock_read!();
            general_fst_access_lock_write!();

            if let Ok(kv_store) = StoreKVPool::acquire(StoreKVAcquireMode::OpenOnly, collection) {
                // Important: acquire bucket store write lock
                let lck_id: String = thread_rng().sample_iter(&Alphanumeric).take(8).collect();
                error!("[flushb_{}_executor_kv_lock_write:{}] ->", collection.as_str(), lck_id);
                executor_kv_lock_write!(kv_store);

                if kv_store.is_some() {
                    // Store exists, proceed erasure.
                    debug!(
                        "collection store exists, erasing: {} from {}",
                        bucket.as_str(),
                        collection.as_str()
                    );

                    let kv_action = StoreKVActionBuilder::access(bucket, kv_store);

                    // Notice: we cannot use the provided KV bucket erasure helper there, as \
                    //   erasing a bucket requires a database lock, which would incur a dead-lock, \
                    //   thus we need to perform the erasure from there.
                    if let Ok(erase_count) = kv_action.batch_erase_bucket() {
                        if StoreFSTActionBuilder::erase(collection, Some(bucket)).is_ok() {
                            debug!("done with bucket erasure");

                            error!("[flushb_{}_executor_kv_lock_write:{}] <-", collection.as_str(), lck_id);
                            return Ok(erase_count);
                        }
                    }
                } else {
                    // Store does not exist, consider as already erased.
                    debug!(
                        "collection store does not exist, consider {} from {} already erased",
                        bucket.as_str(),
                        collection.as_str()
                    );

                    error!("[flushb_{}_executor_kv_lock_write:{}] <-", collection.as_str(), lck_id);
                    return Ok(0);
                }

                error!("[flushb_{}_executor_kv_lock_write:{}] <-", collection.as_str(), lck_id);
            }
        }

        Err(())
    }
}
