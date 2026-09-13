use super::PoolId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tenant {
    pub id: TenantId,
    pub pool_id: PoolId,
    pub ownership_epoch: u64,
}
