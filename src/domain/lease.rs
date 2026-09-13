use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseOwnerId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub owner_id: LeaseOwnerId,
    pub ownership_epoch: u64,
    pub leased_at: SystemTime,
    pub expires_at: SystemTime,
}
