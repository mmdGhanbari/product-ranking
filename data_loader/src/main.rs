use data_loader::extract_last_id;
use diesel_async::{AsyncConnection, AsyncMysqlConnection};
use std::time::UNIX_EPOCH;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn_url = "mysql://remote:dChig96ZjhG5Py9Ni42h@167.235.207.95:3306/waithero";
    let stats_conn_url = "mysql://remote:dChig96ZjhG5Py9Ni42h@5.75.188.165:3306/statistics";

    let mut handles: Vec<JoinHandle<anyhow::Result<()>>> = vec![];

    let start = UNIX_EPOCH.elapsed()?.as_millis();

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_product_restaurants(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/product_restaurant.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_restaurant rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_product_details(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/product_detail.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_detail rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_product_ingredients(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/product_ingredient.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_ingredient rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_product_restaurant_ingredients(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/product_restaurant_ingredient.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_restaurant_ingredient rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_ingredient_allergens(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/ingredient_allergen.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} ingredient_allergens rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_user_preferences(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/user_preferences.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} user_preferences rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_user_allergens(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/user_allergen.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} user_allergen rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_user_favorite_products(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/user_favorite_product.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} user_favorite_product rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(conn_url).await?;
        let rows = data_loader::load_advisor_campaign_products(&mut conn).await?;
        data_loader::write_to_csv_file(&rows, "data/new/advisor_campaign_product.csv", false)?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} advisor_campaign_product rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(stats_conn_url).await?;

        let file_path = "data/new/product_views.csv";
        let last_id = extract_last_id(file_path);

        let rows = data_loader::load_product_views(&mut conn, last_id).await?;
        data_loader::write_to_csv_file(&rows, file_path, last_id.is_some())?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_view rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(stats_conn_url).await?;

        let file_path = "data/new/product_image_views.csv";
        let last_id = extract_last_id(file_path);

        let rows = data_loader::load_product_image_views(&mut conn, last_id).await?;
        data_loader::write_to_csv_file(&rows, file_path, last_id.is_some())?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} product_image_view rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    handles.push(tokio::task::spawn(async move {
        let mut conn = AsyncMysqlConnection::establish(stats_conn_url).await?;

        let file_path = "data/new/category_views.csv";
        let last_id = extract_last_id(file_path);

        let rows = data_loader::load_category_views(&mut conn, last_id).await?;
        data_loader::write_to_csv_file(&rows, file_path, last_id.is_some())?;

        let end = UNIX_EPOCH.elapsed()?.as_millis();
        println!(
            "Fetched and saved {} category_view rows in {}ms",
            rows.len(),
            end - start
        );
        Ok(())
    }));

    for handle in handles {
        handle.await.ok();
    }

    extract_last_id("data/new/category_views.csv");

    Ok(())
}
