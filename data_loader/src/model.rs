use diesel::internal::derives::multiconnection::chrono::NaiveDateTime;
use diesel::sql_types::*;
use diesel::{Insertable, QueryableByName};
use serde::{Serialize, Serializer};

#[derive(Debug, Serialize, QueryableByName)]
pub struct UserAllergen {
    #[diesel(sql_type = BigInt)]
    pub id_user: i64,
    #[diesel(sql_type = BigInt)]
    pub id_allergen: i64,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct UserPreferences {
    #[diesel(sql_type = BigInt)]
    pub id: i64,
    #[diesel(sql_type = TinyInt)]
    pub alcohol: i8,
    #[diesel(sql_type = TinyInt)]
    pub gluten_free: i8,
    #[diesel(sql_type = TinyInt)]
    pub spicy: i8,
    #[diesel(sql_type = TinyInt)]
    pub sugar: i8,
    #[diesel(sql_type = TinyInt)]
    pub vegan: i8,
    #[diesel(sql_type = TinyInt)]
    pub vegetarian: i8,
    #[diesel(sql_type = TinyInt)]
    pub halal: i8,
    #[diesel(sql_type = TinyInt)]
    pub casherut: i8,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct UserFavoriteProduct {
    #[diesel(sql_type = BigInt)]
    pub id_user: i64,
    #[diesel(sql_type = BigInt)]
    pub id_product_restaurant: i64,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct ProductRestaurant {
    #[diesel(sql_type = BigInt)]
    pub id: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub id_product: Option<i64>,
    #[diesel(sql_type = TinyInt)]
    pub highlight: i8,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct ProductDetail {
    #[diesel(sql_type = BigInt)]
    pub id: i64,
    #[diesel(sql_type = TinyInt)]
    pub alcohol: i8,
    #[diesel(sql_type = TinyInt)]
    pub gluten_free: i8,
    #[diesel(sql_type = TinyInt)]
    pub spicy: i8,
    #[diesel(sql_type = TinyInt)]
    pub sugar: i8,
    #[diesel(sql_type = TinyInt)]
    pub vegan: i8,
    #[diesel(sql_type = TinyInt)]
    pub vegetarian: i8,
    #[diesel(sql_type = TinyInt)]
    pub halal: i8,
    #[diesel(sql_type = TinyInt)]
    pub casherut: i8,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct ProductIngredient {
    #[diesel(sql_type = BigInt)]
    pub id_product: i64,
    #[diesel(sql_type = BigInt)]
    pub id_ingredient: i64,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct ProductRestaurantIngredient {
    #[diesel(sql_type = BigInt)]
    pub id_product_restaurant: i64,
    #[diesel(sql_type = BigInt)]
    pub id_ingredient: i64,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct IngredientAllergen {
    #[diesel(sql_type = BigInt)]
    pub id_ingredient: i64,
    #[diesel(sql_type = BigInt)]
    pub id_allergen: i64,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct AdvisorCampaignProduct {
    #[diesel(sql_type = BigInt)]
    pub id_product_restaurant: i64,
}

fn serialize_naive_date_time<S: Serializer>(
    naive_date_time: &NaiveDateTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    naive_date_time
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
        .serialize(serializer)
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct ProductViewAction {
    #[diesel(sql_type = BigInt)]
    pub id: i64,
    #[diesel(sql_type = Text)]
    pub action: String,
    #[diesel(sql_type = BigInt)]
    pub category_id: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub product_id: Option<i64>,
    #[diesel(sql_type = Text)]
    pub mac_address: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub user_id: Option<i64>,
    #[diesel(sql_type = Timestamp)]
    #[serde(serialize_with = "serialize_naive_date_time")]
    pub date_insert: NaiveDateTime,
}

#[derive(Debug, Serialize, QueryableByName)]
pub struct CategoryViewAction {
    #[diesel(sql_type = BigInt)]
    pub id: i64,
    #[diesel(sql_type = Text)]
    pub action: String,
    #[diesel(sql_type = BigInt)]
    pub category_id: i64,
    #[diesel(sql_type = Text)]
    pub mac_address: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub user_id: Option<i64>,
    #[diesel(sql_type = Timestamp)]
    #[serde(serialize_with = "serialize_naive_date_time")]
    pub date_insert: NaiveDateTime,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = crate::schema::users_product_ai)]
pub struct ProductRanking {
    pub id_user: Option<i64>,
    pub mac_address: Option<String>,
    pub id_product: i64,
    pub rank: i32,
}
