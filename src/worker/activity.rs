use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::ActivityType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityError {
    pub message: String,
}

impl ActivityError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ActivityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ActivityError {}

#[async_trait]
pub trait Activity: Send + Sync {
    async fn execute(&self, input: &[u8]) -> Result<Vec<u8>, ActivityError>;
}

#[derive(Clone, Default)]
pub struct ActivityRegistry {
    activities: HashMap<ActivityType, Arc<dyn Activity>>,
}

impl ActivityRegistry {
    pub fn register<A>(&mut self, activity_type: ActivityType, activity: A)
    where
        A: Activity + 'static,
    {
        self.activities.insert(activity_type, Arc::new(activity));
    }

    pub(crate) fn get(&self, activity_type: &ActivityType) -> Option<Arc<dyn Activity>> {
        self.activities.get(activity_type).cloned()
    }
}
