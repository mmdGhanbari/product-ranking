use data_loader::ProductRanking;
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

#[derive(Debug, Clone, Default)]
pub struct UserInfo {
    pub allergens: Vec<usize>,
    pub preferences: Vec<ProductVariant>,
    pub favorite_products: Vec<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UserAllergen {
    pub id_user: usize,
    pub id_allergen: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UserPreferences {
    pub id: usize,
    pub alcohol: u8,
    pub gluten_free: u8,
    pub spicy: u8,
    pub sugar: u8,
    pub vegan: u8,
    pub vegetarian: u8,
    pub halal: u8,
    pub casherut: u8,
    #[serde(skip)]
    pub variants: Vec<ProductVariant>,
}

impl UserPreferences {
    pub fn populate_variants(&mut self) {
        if self.alcohol > 0 {
            self.variants.push(ProductVariant::Alcohol);
        } else if self.gluten_free > 0 {
            self.variants.push(ProductVariant::GlutenFree);
        } else if self.spicy > 0 {
            self.variants.push(ProductVariant::Spicy);
        } else if self.sugar > 0 {
            self.variants.push(ProductVariant::Sugar);
        } else if self.vegan > 0 {
            self.variants.push(ProductVariant::Vegan);
        } else if self.vegetarian > 0 {
            self.variants.push(ProductVariant::Vegetarian);
        } else if self.halal > 0 {
            self.variants.push(ProductVariant::Halal);
        } else if self.casherut > 0 {
            self.variants.push(ProductVariant::Casherut);
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct UserFavoriteProduct {
    pub id_user: usize,
    pub id_product_restaurant: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductRestaurant {
    pub id: usize,
    #[serde(alias = "id_product")]
    pub id_product_master: Option<usize>,
    pub highlight: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ProductVariant {
    Alcohol,
    GlutenFree,
    Spicy,
    Sugar,
    Vegan,
    Vegetarian,
    Halal,
    Casherut,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductDetails {
    #[serde(alias = "id")]
    pub product_id: usize,
    pub alcohol: u8,
    pub gluten_free: u8,
    pub spicy: u8,
    pub sugar: u8,
    pub vegan: u8,
    pub vegetarian: u8,
    pub halal: u8,
    pub casherut: u8,
    #[serde(skip)]
    pub variants: Vec<ProductVariant>,
}

impl ProductDetails {
    pub fn populate_variants(&mut self) {
        if self.alcohol > 0 {
            self.variants.push(ProductVariant::Alcohol);
        } else if self.gluten_free > 0 {
            self.variants.push(ProductVariant::GlutenFree);
        } else if self.spicy > 0 {
            self.variants.push(ProductVariant::Spicy);
        } else if self.sugar > 0 {
            self.variants.push(ProductVariant::Sugar);
        } else if self.vegan > 0 {
            self.variants.push(ProductVariant::Vegan);
        } else if self.vegetarian > 0 {
            self.variants.push(ProductVariant::Vegetarian);
        } else if self.halal > 0 {
            self.variants.push(ProductVariant::Halal);
        } else if self.casherut > 0 {
            self.variants.push(ProductVariant::Casherut);
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductIngredient {
    pub id_product: usize,
    pub id_ingredient: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductRestaurantIngredient {
    pub id_product_restaurant: usize,
    pub id_ingredient: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IngredientAllergen {
    pub id_ingredient: usize,
    pub id_allergen: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AdvisorCampaignProduct {
    pub id_product_restaurant: usize,
}

#[derive(Debug)]
pub struct Ranking {
    pub user: User,
    pub product_id: usize,
    pub rank: usize,
}

impl Into<ProductRanking> for &Ranking {
    fn into(self) -> ProductRanking {
        let serializable: SerializableRanking = self.into();
        ProductRanking {
            id_user: serializable.user_id.map(|id| id as i64),
            mac_address: Some(serializable.mac_address),
            id_product: self.product_id as i64,
            rank: self.rank as i32,
        }
    }
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
