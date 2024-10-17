use chrono::{Duration, Utc};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::usize;

use crate::data::{
    process_view_csv, read_advisor_campaign_products, read_ingredient_allergens,
    read_product_details, read_product_ingredients, read_product_restaurant_ingredients,
    read_product_restaurants, read_users_info, write_rankings,
};
use crate::model::{ProductDetails, ProductRestaurant, ProductVariant, UserId, UserInfo};
use model::{Ranking, ViewScope};

mod data;
mod model;
mod sample;

fn compute_product_ingredients(
    id: usize,
    product_restaurants: &HashMap<usize, ProductRestaurant>,
    product_restaurant_ingredients: &HashMap<usize, HashSet<usize>>,
    product_ingredients: &HashMap<usize, HashSet<usize>>,
) -> HashSet<usize> {
    let mut ingredients = HashSet::new();
    let master_product_id = product_restaurants
        .get(&id)
        .and_then(|pr| pr.id_product_master);

    if let Some(master_ingredients) = master_product_id.and_then(|id| product_ingredients.get(&id))
    {
        ingredients.extend(master_ingredients);
    }
    if let Some(product_ingredients) = product_restaurant_ingredients.get(&id) {
        ingredients.extend(product_ingredients);
    }

    ingredients
}

fn user_is_allergic_to_product(
    user_info: &UserInfo,
    product_id: usize,
    product_restaurant_ingredients: &HashMap<usize, HashSet<usize>>,
    ingredient_allergens: &HashMap<usize, HashSet<usize>>,
) -> bool {
    let Some(ingredients) = product_restaurant_ingredients.get(&product_id) else {
        return false;
    };

    let empty: HashSet<usize> = HashSet::new();
    let allergens: HashSet<&usize> = ingredients
        .iter()
        .flat_map(|ingredient| ingredient_allergens.get(ingredient).unwrap_or(&empty))
        .collect();

    user_info
        .allergens
        .iter()
        .any(|allergen| allergens.contains(allergen))
}

fn user_prefers_product(
    user_info: &UserInfo,
    product_id: usize,
    product_details: &HashMap<usize, ProductDetails>,
) -> bool {
    let Some(details) = product_details.get(&product_id) else {
        return false;
    };

    details
        .variants
        .iter()
        .any(|variant| user_info.preferences.contains(variant))
}

fn user_likes_product(user_info: &UserInfo, product_id: usize) -> bool {
    user_info.favorite_products.contains(&product_id)
}

#[tokio::main]
async fn main() {
    let start = Utc::now();

    // For each csv file, we spawn a new thread
    let product_view_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/new/product_views.csv").expect("to process product csv")
    });
    let product_image_view_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/new/product_image_views.csv").expect("to process product_image csv")
    });
    let category_view_proc_handle = std::thread::spawn(|| {
        process_view_csv("data/new/category_views.csv").expect("to process category csv")
    });
    let users_info_proc_handle = std::thread::spawn(|| {
        read_users_info(
            "data/new/user_allergen.csv",
            "data/new/user_preferences.csv",
            "data/new/user_favorite_product.csv",
        )
        .expect("to read users info")
    });
    let product_restaurants_proc_handle = std::thread::spawn(|| {
        read_product_restaurants("data/new/product_restaurant.csv")
            .expect("to read product_restaurant csv")
    });
    let product_details_proc_handle = std::thread::spawn(|| {
        read_product_details("data/new/product_detail.csv").expect("to read details csv")
    });
    let product_ingredients_proc_handle = std::thread::spawn(|| {
        read_product_ingredients("data/new/product_ingredient.csv")
            .expect("to read product_ingredients csv")
    });
    let product_restaurant_ingredients_proc_handle = std::thread::spawn(|| {
        read_product_restaurant_ingredients("data/new/product_restaurant_ingredient.csv")
            .expect("to read product_restaurant_ingredients csv")
    });
    let ingredient_allergens_proc_handle = std::thread::spawn(|| {
        read_ingredient_allergens("data/new/ingredient_allergen.csv")
            .expect("to read ingredient allergens csv")
    });
    let advisor_campaign_products_proc_handle = std::thread::spawn(|| {
        read_advisor_campaign_products("data/new/advisor_campaign_product.csv")
            .expect("to read advisor_campaign_product csv")
    });

    // Wait for the threads to finish calculating...
    let (
        product_views,
        product_image_views,
        category_views,
        users_info,
        product_restaurants,
        product_details,
        product_ingredients,
        product_restaurant_ingredients,
        ingredient_allergens,
        advisor_campaign_products,
    ) = (
        product_view_proc_handle
            .join()
            .expect("to process products"),
        product_image_view_proc_handle
            .join()
            .expect("to process product images"),
        category_view_proc_handle
            .join()
            .expect("to process categories"),
        users_info_proc_handle
            .join()
            .expect("to process users info"),
        product_restaurants_proc_handle
            .join()
            .expect("to read product_restaurants"),
        product_details_proc_handle.join().expect("to read details"),
        product_ingredients_proc_handle
            .join()
            .expect("to read ingredients"),
        product_restaurant_ingredients_proc_handle
            .join()
            .expect("to read ingredients"),
        ingredient_allergens_proc_handle
            .join()
            .expect("to read ingredient allergens"),
        advisor_campaign_products_proc_handle
            .join()
            .expect("to read advisor_campaign_products"),
    );

    let mut rankings: Vec<Ranking> = Vec::with_capacity(100_000);

    // Iterate on users...
    for (user, product_views) in product_views.iter() {
        let user_info = match user.user_id {
            UserId::Value(id) => users_info.get(&id).cloned(),
            UserId::Empty(_) => None,
        }
        .unwrap_or_default();

        let empty_map: HashMap<ViewScope, Duration> = HashMap::new();
        let image_views = product_image_views.get(user).unwrap_or(&empty_map);
        let category_views = category_views.get(user).unwrap_or(&empty_map);

        // Holds the total view duration for each ingredient
        let mut ingredients_views: HashMap<usize, Duration> = HashMap::new();

        // Holds the total view duration for each variant
        let mut variants_views: HashMap<ProductVariant, Duration> = HashMap::new();

        let mut master_products_views: HashMap<usize, Duration> = HashMap::new();
        let mut master_products_image_views: HashMap<usize, Duration> = HashMap::new();

        // Iterate on the products views of that user...
        for (product, &product_view) in product_views.iter() {
            let Some(product_id) = product.product_id else {
                panic!("No product id, cat_id: {}", product.category_id);
            };
            let master_product_id = product_restaurants
                .get(&product_id)
                .and_then(|pr| pr.id_product_master);

            let ingredients = compute_product_ingredients(
                product_id,
                &product_restaurants,
                &product_restaurant_ingredients,
                &product_ingredients,
            );

            if !ingredients.is_empty() {
                // Update the view duration for each ingredient
                ingredients.iter().for_each(|ingredient_id| {
                    let ingredient_entry = ingredients_views.entry(*ingredient_id).or_default();
                    *ingredient_entry += product_view;
                });
            }

            // Update the view duration for this product's variants
            if let Some(variants) = master_product_id
                .and_then(|master_product_id| product_details.get(&master_product_id))
                .map(|details| details.variants.clone())
            {
                for variant in variants {
                    let variant_entry = variants_views.entry(variant).or_default();
                    *variant_entry += product_view;
                }
            }

            if let Some(master_id) = master_product_id {
                *master_products_views.entry(master_id).or_default() += product_view;

                let image_view = image_views.get(product).cloned().unwrap_or_default();
                *master_products_image_views.entry(master_id).or_default() += image_view;
            }
        }

        let total_variants_view: usize = variants_views
            .values()
            .map(|d| d.num_seconds() as usize)
            .sum();

        // Iterate on the products views of that user...
        for (product, &product_view) in product_views.iter() {
            let Some(product_id) = product.product_id else {
                panic!("No product id, cat_id: {}", product.category_id);
            };
            let product_info = product_restaurants.get(&product_id);

            let master_product_id = product_info.and_then(|pr| pr.id_product_master);

            let highlighted = product_info.map(|pr| pr.highlight > 0).unwrap_or(false);
            let liked = user_likes_product(&user_info, product_id);
            let in_campaign = advisor_campaign_products.contains(&product_id);

            let rank_booster: i64 = vec![highlighted, liked, in_campaign]
                .iter()
                .map(|val| if *val { 1 } else { 0 })
                .sum();

            let mut user_factor = 1i64;
            if user_is_allergic_to_product(
                &user_info,
                product_id,
                &product_restaurant_ingredients,
                &ingredient_allergens,
            ) {
                user_factor = 0;
            }
            if user_prefers_product(&user_info, product_id, &product_details) {
                user_factor *= 10;
            }

            if user_factor == 0 {
                rankings.push(Ranking {
                    user: user.clone(),
                    product_id,
                    rank: 0,
                });
                continue;
            }

            let product_view = master_product_id
                .and_then(|id| master_products_views.get(&id))
                .cloned()
                .unwrap_or(product_view);

            // Get product_image view duration
            let image_view = master_product_id
                .and_then(|id| master_products_image_views.get(&id))
                .cloned()
                .unwrap_or(image_views.get(product).cloned().unwrap_or_default());

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
            let ingredients = compute_product_ingredients(
                product_id,
                &product_restaurants,
                &product_restaurant_ingredients,
                &product_ingredients,
            );
            let ingredient_ranking: i64 = if !ingredients.is_empty() {
                let ingredients_total_millis: f32 = ingredients
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
                ((ingredients_total_millis / 1000f32) / (ingredients.len() as f32)) as i64
            } else {
                // If there's no ingredient for this product, ingredient ranking will be 0
                0
            };

            // Calculate the product's variants weight
            let variants_weight: f32 = {
                if let Some(variants) = master_product_id
                    .and_then(|master_product_id| product_details.get(&master_product_id))
                    .map(|details| &details.variants)
                {
                    variants
                        .iter()
                        .map(|variant| {
                            let variant_duration =
                                variants_views.get(&variant).cloned().unwrap_or_default();
                            (variant_duration.num_seconds() as f32) / (total_variants_view as f32)
                        })
                        .sum()
                } else {
                    0f32
                }
            };

            // Calculate the ranking
            let mut rank = 5 * image_view.num_seconds()
                + ((1f32 + variants_weight) * product_view.num_seconds() as f32) as i64
                + (0.1 * category_view.num_seconds() as f32) as i64
                + ingredient_ranking;
            rank *= user_factor;
            rank *= max(1, rank_booster * 1000);

            rankings.push(Ranking {
                user: user.clone(),
                product_id,
                rank: rank as usize,
            })
        }
    }

    println!("Done in {}ms", (Utc::now() - start).num_milliseconds());
    
    // Write calculated rankings into the output file
    write_rankings("data/new/out.csv", &rankings).unwrap();
    
    data_loader::save_rankings_to_db(&rankings).await.unwrap();
}
