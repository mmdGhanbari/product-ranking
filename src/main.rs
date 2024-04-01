use std::{
    collections::{HashMap, HashSet},
    fs::File,
};

use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use csv::{ReaderBuilder, WriterBuilder};

use model::{
    Ranking, SerializableRanking, User, ViewAction, ViewIdentifier, ViewRecord, ViewScope,
};

use crate::model::{ProductDetails, ProductIngredient, ProductMapping, ProductVariant};

mod model;
mod sample;

const MAX_MISSING_DURATION: Duration = Duration::seconds(30);

/*
    For each csv file, we have a Map like this:
    {
        [user]: {
            [scope]: Duration
        }
    }
*/
type UserViews = HashMap<User, HashMap<ViewScope, Duration>>;

fn update_user_view(views: &mut UserViews, user: &User, scope: &ViewScope, duration: Duration) {
    let user_entry = views.entry(user.clone()).or_default();
    let view_entry = user_entry.entry(scope.clone()).or_default();

    *view_entry += duration;
}

fn process_view_csv(file_path: &str) -> Result<HashMap<User, HashMap<ViewScope, Duration>>> {
    // We keep the time of last OPEN action for each (user, scope)
    let mut open_times: HashMap<ViewIdentifier, DateTime<Utc>> = HashMap::new();
    let mut views: UserViews = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ViewRecord>() {
        // Deserialize the record
        let record = record?;
        let iden = ViewIdentifier(record.user.clone(), record.scope.clone());
        let date_insert = NaiveDateTime::parse_from_str(&record.date_insert, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc();

        match record.action {
            // If it's an OPEN action...
            ViewAction::Open => {
                // If the previous action was also open, we consider it as a non-closed view
                if let Some(open_time) = open_times.remove(&iden) {
                    let duration =
                        (Utc::now() - open_time).clamp(Duration::seconds(0), MAX_MISSING_DURATION);
                    update_user_view(&mut views, &record.user, &record.scope, duration);
                }
                // Store the open time
                open_times.insert(iden, date_insert);
            }
            // If it's an CLOSE action...
            ViewAction::Close => {
                if let Some(open_time) = open_times.remove(&iden) {
                    let duration = date_insert - open_time;
                    update_user_view(&mut views, &record.user, &record.scope, duration);
                }
            }
        }
    }

    // If there's any OPEN action that was never closed, we handle it here...
    for (ViewIdentifier(user, scope), open_time) in open_times.iter() {
        let duration = (Utc::now() - open_time).clamp(Duration::seconds(0), MAX_MISSING_DURATION);
        update_user_view(&mut views, user, scope, duration);
    }

    Ok(views)
}

// Read the ingredients of each product
fn read_ingredients(file_path: &str) -> Result<HashMap<usize, HashSet<usize>>> {
    // A map from product_id to HashSet<ingredient_id>
    // We use HashSet to make sure that each ingredient appears only once in each product
    let mut ingredients = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductIngredient>() {
        let record = record?;
        let product_entry: &mut HashSet<usize> = ingredients.entry(record.product_id).or_default();
        product_entry.insert(record.ingredient_id);
    }

    Ok(ingredients)
}

fn read_product_mappings(file_path: &str) -> Result<HashMap<usize, usize>> {
    let mut mapping = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductMapping>() {
        let record = record?;
        if let Some(master_product) = record.master_product {
            mapping.insert(record.restaurant_product, master_product);
        }
    }

    Ok(mapping)
}

fn read_product_details(file_path: &str) -> Result<HashMap<usize, ProductDetails>> {
    let mut details = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductDetails>() {
        let mut record = record?;
        record.variant = if record.alcohol > 0 {
            Some(ProductVariant::Alcohol)
        } else if record.gluten_free > 0 {
            Some(ProductVariant::GlutenFree)
        } else if record.spicy > 0 {
            Some(ProductVariant::Spicy)
        } else if record.sugar > 0 {
            Some(ProductVariant::Sugar)
        } else if record.vegan > 0 {
            Some(ProductVariant::Vegan)
        } else if record.vegetarian > 0 {
            Some(ProductVariant::Vegetarian)
        } else if record.halal > 0 {
            Some(ProductVariant::Halal)
        } else if record.casherut > 0 {
            Some(ProductVariant::Casherut)
        } else {
            None
        };
        details.insert(record.product_id, record);
    }

    Ok(details)
}

// Write calculated rankings into the output file
fn write_rankings(file_path: &str, rankings: &[Ranking]) -> Result<()> {
    let file = File::create(file_path)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    for ranking in rankings {
        writer
            .serialize::<SerializableRanking>(ranking.into())
            .expect("to serialize ranking");
    }

    writer.flush()?;

    Ok(())
}

fn main() {
    let start = Utc::now();

    // For each csv file, we spawn a new thread
    let product_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/product_views.csv").expect("to process product csv")
    });
    let product_image_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/product_image_views.csv").expect("to process product_image csv")
    });
    let category_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/category_views.csv").expect("to process category csv")
    });
    let ingredients_proc_handle = std::thread::spawn(|| {
        read_ingredients("data/product_ingredient.csv").expect("to read ingredients csv")
    });
    let product_mappings_proc_handle = std::thread::spawn(|| {
        read_product_mappings("data/product_mapping.csv").expect("to read mappings csv")
    });
    let product_details_proc_handle = std::thread::spawn(|| {
        read_product_details("data/product_detail.csv").expect("to read details csv")
    });

    // Wait for the threads to finish calculating...
    let (
        product_views,
        product_image_views,
        category_views,
        ingredients,
        product_mappings,
        product_details,
    ) = (
        product_proc_handle.join().expect("to process products"),
        product_image_proc_handle
            .join()
            .expect("to process product images"),
        category_proc_handle.join().expect("to process categories"),
        ingredients_proc_handle.join().expect("to read ingredients"),
        product_mappings_proc_handle
            .join()
            .expect("to read mappings"),
        product_details_proc_handle.join().expect("to read details"),
    );

    let mut rankings: Vec<Ranking> = Vec::with_capacity(100_000);

    // Iterate on users...
    for (user, product_views) in product_views.iter() {
        let empty_map: HashMap<ViewScope, Duration> = HashMap::new();
        let image_views = product_image_views.get(user).unwrap_or(&empty_map);
        let category_views = category_views.get(user).unwrap_or(&empty_map);

        // Holds the total view duration for each ingredient
        let mut ingredients_views: HashMap<usize, Duration> = HashMap::new();

        // Holds the total view duration for each variant
        let mut variants_views: HashMap<ProductVariant, Duration> = HashMap::new();

        // Iterate on the products views of that user...
        for (product, &product_view) in product_views.iter() {
            let product_id = product.product_id.expect("to have product_id");
            let master_product_id = product_mappings.get(&product_id);

            let Some(product_ingredients) = ingredients.get(&product_id) else {
                continue;
            };

            // Update the view duration for each ingredient
            product_ingredients.iter().for_each(|ingredient_id| {
                let ingredient_entry = ingredients_views.entry(*ingredient_id).or_default();
                *ingredient_entry += product_view;
            });

            // Update the view duration for this product's variant
            if let Some(master_product_id) = master_product_id {
                let details = product_details.get(master_product_id).cloned();
                if let Some(details) = details {
                    if let Some(variant) = details.variant {
                        let variant_entry = variants_views.entry(variant).or_default();
                        *variant_entry += product_view;
                    }
                }
            }
        }

        let total_variants_view: usize = variants_views
            .values()
            .map(|d| d.num_seconds() as usize)
            .sum();

        // Iterate on the products views of that user...
        for (product, &product_view) in product_views.iter() {
            let product_id = product.product_id.expect("to have product_id");

            // Get product_image view duration
            let image_view = image_views.get(product).cloned().unwrap_or_default();

            // Get category view duration
            let category_scope = ViewScope {
                category_id: product.category_id.clone(),
                product_id: None,
            };
            let category_view = category_views
                .get(&category_scope)
                .cloned()
                .unwrap_or_default();

            // Calculate the ranking coming from the ingredients
            let ingredient_ranking: i64 = if let Some(product_ingredients) =
                ingredients.get(&product_id)
            {
                let ingredients_total_millis: f32 = product_ingredients
                    .iter()
                    .map(|ingredient_id| {
                        ingredients_views
                            .get(ingredient_id)
                            .cloned()
                            .unwrap_or_default()
                            .num_milliseconds() as f32
                    })
                    .sum();

                // Total seconds of ingredients views / number of ingredients
                ((ingredients_total_millis / 1000f32) / (product_ingredients.len() as f32)) as i64
            } else {
                // If there's no ingredient for this product, ingredient ranking will be 0
                0
            };

            // Calculate the product's variant weight
            let variant_weight: f32 = {
                let master_product_id = product_mappings.get(&product_id);
                if let Some(master_product_id) = master_product_id {
                    let details = product_details.get(master_product_id).cloned();
                    if let Some(details) = details {
                        if let Some(variant) = details.variant {
                            let variant_duration =
                                variants_views.get(&variant).cloned().unwrap_or_default();
                            (variant_duration.num_seconds() as f32) / (total_variants_view as f32)
                        } else {
                            0f32
                        }
                    } else {
                        0f32
                    }
                } else {
                    0f32
                }
            };

            // Calculate the ranking
            let rank = 5 * image_view.num_seconds()
                + ((1f32 + variant_weight) * product_view.num_seconds() as f32) as i64
                + (0.1 * category_view.num_seconds() as f32) as i64
                + ingredient_ranking;

            rankings.push(Ranking {
                user: user.clone(),
                product_id,
                rank: rank as usize,
            })
        }
    }

    // Write calculated rankings into the output file
    write_rankings("data/out.csv", &rankings).unwrap();

    println!("Done in {}ms", (Utc::now() - start).num_milliseconds());
}
