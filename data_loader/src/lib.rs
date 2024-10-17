use csv::WriterBuilder;
use diesel::dsl::insert_into;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel_async::{AsyncConnection, AsyncMysqlConnection, RunQueryDsl};
use itertools::Itertools;
pub use model::*;
use rev_buf_reader::RevBufReader;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::BufRead;

mod model;
mod schema;

pub async fn load_product_restaurants(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<ProductRestaurant>> {
    let query = "
        select
            id,
            id_product,
            highlight
        from product_restaurant
        where deleted = 0;";
    sql_query(query)
        .load::<ProductRestaurant>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_product_details(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<ProductDetail>> {
    let query = "
        select
            id,
            alcohol,
            gluten_free,
            spicy,
            sugar,
            vegan,
            vegetarian,
            halal,
            casherut
        from product_detail
        where deleted = 0;";
    sql_query(query)
        .load::<ProductDetail>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_product_ingredients(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<ProductIngredient>> {
    let query = "
        select
            id_product,
            id_ingredient
        from product_ingredient
        where deleted = 0;";
    sql_query(query)
        .load::<ProductIngredient>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_product_restaurant_ingredients(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<ProductRestaurantIngredient>> {
    let query = "
        select
            id_product_restaurant,
            id_ingredient
        from product_restaurant_ingredient
        where deleted = 0;";
    sql_query(query)
        .load::<ProductRestaurantIngredient>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_ingredient_allergens(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<IngredientAllergen>> {
    let query = "
        select
            id_ingredient,
            id_allergen
        from ingredient_allergen
        where deleted = 0;";
    sql_query(query)
        .load::<IngredientAllergen>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_user_preferences(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<UserPreferences>> {
    let query = "
        select
            id,
            alcohol,
            gluten_free,
            spicy,
            sugar,
            vegan,
            vegetarian,
            halal,
            casherut
        from users_preferences;";
    sql_query(query)
        .load::<UserPreferences>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_user_allergens(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<UserAllergen>> {
    let query = "
        select
            id_user,
            id_allergen
        from users_allergen
        where deleted = 0;";
    sql_query(query)
        .load::<UserAllergen>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_user_favorite_products(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<UserFavoriteProduct>> {
    let query = "
        select
            id_user,
            id_product_restaurant
        from users_product_favorite
        where deleted = 0;";
    sql_query(query)
        .load::<UserFavoriteProduct>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_advisor_campaign_products(
    conn: &mut AsyncMysqlConnection,
) -> anyhow::Result<Vec<AdvisorCampaignProduct>> {
    let query = "
        select distinct id_product_restaurant
        from advisor_campaign_product
        where deleted = 0;";

    sql_query(query)
        .load::<AdvisorCampaignProduct>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_product_views(
    conn: &mut AsyncMysqlConnection,
    last_id: Option<i64>,
) -> anyhow::Result<Vec<ProductViewAction>> {
    let query = "
        select
            id,
            action,
            category_id,
            product_id,
            mac_address,
            user_id,
            date_insert
        from product_views
        where id > ?;";
    sql_query(query)
        .bind::<BigInt, _>(last_id.unwrap_or(0))
        .load::<ProductViewAction>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_product_image_views(
    conn: &mut AsyncMysqlConnection,
    last_id: Option<i64>,
) -> anyhow::Result<Vec<ProductViewAction>> {
    let query = "
        select
            id,
            action,
            category_id,
            product_id,
            mac_address,
            user_id,
            date_insert
        from product_image_views
        where id > ?;";
    sql_query(query)
        .bind::<BigInt, _>(last_id.unwrap_or(0))
        .load::<ProductViewAction>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub async fn load_category_views(
    conn: &mut AsyncMysqlConnection,
    last_id: Option<i64>,
) -> anyhow::Result<Vec<CategoryViewAction>> {
    let query = "
        select
            id,
            action,
            category_id,
            mac_address,
            user_id,
            date_insert
        from category_views
        where id > ?;";
    sql_query(query)
        .bind::<BigInt, _>(last_id.unwrap_or(0))
        .load::<CategoryViewAction>(conn)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

pub fn write_to_csv_file<T: Serialize>(data: &[T], path: &str, append: bool) -> anyhow::Result<()> {
    let file = if append {
        OpenOptions::new().append(true).open(path)?
    } else {
        File::create(path)?
    };
    let mut wtr = WriterBuilder::new().has_headers(!append).from_writer(file);

    for record in data {
        wtr.serialize(record)?;
    }
    wtr.flush()?;

    Ok(())
}

pub fn extract_last_id(path: &str) -> Option<i64> {
    let Ok(file) = File::open(path) else {
        return None;
    };

    let reader = RevBufReader::new(file);
    let last_lines: Vec<String> = reader
        .lines()
        .take(1)
        .map(|l| l.expect("Could not parse line"))
        .collect();
    let line = last_lines.get(0)?;

    let parts = line.split(',').collect::<Vec<&str>>();
    if parts.len() == 0 {
        return None;
    }

    let id = parts[0].parse::<i64>().ok()?;
    Some(id)
}

pub async fn save_rankings_to_db<E>(records: &[E]) -> anyhow::Result<()>
where
    for<'a> &'a E: Into<ProductRanking>,
{
    let conn_url = "mysql://remote:dChig96ZjhG5Py9Ni42h@167.235.207.95:3306/waithero";
    let mut conn = AsyncMysqlConnection::establish(conn_url).await?;

    println!(">>>>>>> Deleting users_product_ai entries...");

    sql_query("delete from users_product_ai;")
        .execute(&mut conn)
        .await?;
    
    use crate::schema::users_product_ai::dsl::*;
    
    let entries: Vec<ProductRanking> = records.iter().map(|r| r.into()).collect();
    for (idx, entries) in entries.into_iter().chunks(10_000).into_iter().enumerate() {
        println!(">>>>>>> Inserting users_product_ai | Chunk #{}", idx);
    
        let entries: Vec<ProductRanking> = entries.collect();
        insert_into(users_product_ai)
            .values(&entries)
            .execute(&mut conn)
            .await?;
    }

    println!(">>>>>>> Done inserting users_product_ai entries!");

    Ok(())
}
