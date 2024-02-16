use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ViewAction {
    Open,
    Close,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum UserId {
    Value(usize),
    Empty(String),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct User {
    pub mac_address: String,
    pub user_id: UserId,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct ViewScope {
    pub category_id: usize,
    pub product_id: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ViewRecord {
    pub action: ViewAction,
    #[serde(flatten)]
    pub scope: ViewScope,
    #[serde(flatten)]
    pub user: User,
    pub date_insert: String,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct ViewIdentifier(pub User, pub ViewScope);

#[derive(Debug)]
pub struct Ranking {
    pub user: User,
    pub product_id: usize,
    pub rank: usize,
}

#[derive(Debug, Serialize)]
pub struct SerializableRanking {
    pub mac_address: String,
    pub user_id: Option<usize>,
    pub product_id: usize,
    pub rank: usize,
}

impl From<&Ranking> for SerializableRanking {
    fn from(ranking: &Ranking) -> Self {
        let user_id = match ranking.user.user_id {
            UserId::Value(value) => Some(value),
            UserId::Empty(_) => None,
        };
        SerializableRanking {
            mac_address: ranking.user.mac_address.clone(),
            user_id,
            product_id: ranking.product_id,
            rank: ranking.rank,
        }
    }
}
