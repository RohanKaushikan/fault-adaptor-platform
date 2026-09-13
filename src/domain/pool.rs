#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PoolId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pool {
    pub id: PoolId,
    pub name: String,
}
