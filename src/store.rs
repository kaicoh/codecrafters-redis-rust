use super::{RedisError, RedisResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Store {
    inner: Arc<Mutex<HashMap<String, String>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &str) -> RedisResult<Option<String>> {
        self.lock().map(|inner| inner.get(key).cloned())
    }

    pub fn set(&self, key: &str, value: String, exp: Option<u64>) -> RedisResult<()> {
        {
            let mut inner = self.lock()?;
            inner.insert(key.to_string(), value);
        }

        if let Some(exp) = exp {
            let store = self.clone();
            let key = key.to_string();

            thread::spawn(move || {
                thread::sleep(Duration::from_millis(exp));

                match store.lock() {
                    Ok(mut inner) => {
                        inner.remove(&key);
                    }
                    Err(err) => {
                        eprintln!("{err}");
                    }
                }
            });
        }

        Ok(())
    }

    fn lock(&self) -> RedisResult<MutexGuard<HashMap<String, String>>> {
        self.inner
            .lock()
            .map_err(|e| RedisError::Lock(format!("{e}")))
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
