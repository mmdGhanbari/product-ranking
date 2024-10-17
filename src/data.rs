use crate::model::{
    AdvisorCampaignProduct, IngredientAllergen, ProductDetails, ProductIngredient,
    ProductRestaurant, ProductRestaurantIngredient, Ranking, SerializableRanking, User,
    UserAllergen, UserFavoriteProduct, UserId, UserInfo, UserPreferences, ViewAction,
    ViewIdentifier, ViewRecord, ViewScope,
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::{HashMap, HashSet};
use std::fs::File;

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

pub fn process_view_csv(
    file_path: &str,
) -> anyhow::Result<HashMap<User, HashMap<ViewScope, Duration>>> {
    // We keep the time of last OPEN action for each (user, scope)
    let mut open_times: HashMap<ViewIdentifier, DateTime<Utc>> = HashMap::new();
    let mut views: UserViews = HashMap::new();
    // Keep a mapping from mac address to user_id
    let mut mac_addr_user: HashMap<String, usize> = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ViewRecord>() {
        let record: ViewRecord = record?;
        if let UserId::Value(user_id) = record.user.user_id {
            mac_addr_user.insert(record.user.mac_address, user_id);
        }
    }

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);
    
    for record in csv_reader.deserialize::<ViewRecord>() {
        // Deserialize the record
        let record = record?;

        // If there's a mapping for this mac_address to user_id, use that!
        let user = match record.user.user_id {
            UserId::Value(_) => record.user,
            UserId::Empty(_) => {
                let mac_address = &record.user.mac_address;
                if let Some(user_id) = mac_addr_user.get(mac_address) {
                    User {
                        mac_address: mac_address.to_owned(),
                        user_id: UserId::Value(*user_id),
                    }
                } else {
                    record.user
                }
            }
        };

        let iden = ViewIdentifier(user.clone(), record.scope.clone());
        let date_insert =
            NaiveDateTime::parse_from_str(&record.date_insert, "%Y-%m-%d %H:%M:%S")?.and_utc();

        match record.action {
            // If it's an OPEN action...
            ViewAction::Open => {
                // If the previous action was also open, we consider it as a non-closed view
                if let Some(open_time) = open_times.remove(&iden) {
                    let duration =
                        (Utc::now() - open_time).clamp(Duration::seconds(0), MAX_MISSING_DURATION);
                    update_user_view(&mut views, &user, &record.scope, duration);
                }
                // Store the open time
                open_times.insert(iden, date_insert);
            }
            // If it's an CLOSE action...
            ViewAction::Close => {
                if let Some(open_time) = open_times.remove(&iden) {
                    let duration = date_insert - open_time;
                    update_user_view(&mut views, &user, &record.scope, duration);
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

pub fn read_users_info(
    user_allergen_path: &str,
    user_pref_path: &str,
    user_fav_path: &str,
) -> anyhow::Result<HashMap<usize, UserInfo>> {
    let mut users_info: HashMap<usize, UserInfo> = HashMap::new();

    {
        let file = File::open(user_allergen_path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        for record in reader.deserialize::<UserAllergen>() {
            let record = record?;
            let user_entry = users_info.entry(record.id_user).or_default();
            user_entry.allergens.push(record.id_allergen);
        }
    }

    {
        let file = File::open(user_pref_path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        for record in reader.deserialize::<UserPreferences>() {
            let mut record = record?;
            record.populate_variants();

            let user_entry = users_info.entry(record.id).or_default();
            user_entry.preferences = record.variants;
        }
    }

    {
        let file = File::open(user_fav_path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        for record in reader.deserialize::<UserFavoriteProduct>() {
            let record = record?;

            let user_entry = users_info.entry(record.id_user).or_default();
            user_entry
                .favorite_products
                .push(record.id_product_restaurant);
        }
    }

    Ok(users_info)
}

pub fn read_product_restaurants(
    file_path: &str,
) -> anyhow::Result<HashMap<usize, ProductRestaurant>> {
    let mut mapping = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductRestaurant>() {
        let record = record?;
        mapping.insert(record.id, record);
    }

    Ok(mapping)
}

pub fn read_product_details(file_path: &str) -> anyhow::Result<HashMap<usize, ProductDetails>> {
    let mut details = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductDetails>() {
        let mut record = record?;
        record.populate_variants();

        details.insert(record.product_id, record);
    }

    Ok(details)
}

// Read the ingredients of each master product
pub fn read_product_ingredients(file_path: &str) -> anyhow::Result<HashMap<usize, HashSet<usize>>> {
    // A map from product_id to HashSet<ingredient_id>
    // We use HashSet to make sure that each ingredient appears only once in each product
    let mut ingredients = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductIngredient>() {
        let record = record?;
        let product_entry: &mut HashSet<usize> = ingredients.entry(record.id_product).or_default();
        product_entry.insert(record.id_ingredient);
    }

    Ok(ingredients)
}

// Read the ingredients of each product_restaurant
pub fn read_product_restaurant_ingredients(
    file_path: &str,
) -> anyhow::Result<HashMap<usize, HashSet<usize>>> {
    // A map from product_id to HashSet<ingredient_id>
    // We use HashSet to make sure that each ingredient appears only once in each product
    let mut ingredients = HashMap::new();

    // Open the csv file and set up the reader
    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<ProductRestaurantIngredient>() {
        let record = record?;
        let product_entry: &mut HashSet<usize> =
            ingredients.entry(record.id_product_restaurant).or_default();
        product_entry.insert(record.id_ingredient);
    }

    Ok(ingredients)
}

pub fn read_ingredient_allergens(
    file_path: &str,
) -> anyhow::Result<HashMap<usize, HashSet<usize>>> {
    let mut ingredients = HashMap::new();

    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<IngredientAllergen>() {
        let record = record?;
        let ingredient_entry: &mut HashSet<usize> =
            ingredients.entry(record.id_ingredient).or_default();
        ingredient_entry.insert(record.id_allergen);
    }

    Ok(ingredients)
}

pub fn read_advisor_campaign_products(file_path: &str) -> anyhow::Result<HashSet<usize>> {
    let mut products = HashSet::new();

    let file = File::open(file_path)?;
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    for record in csv_reader.deserialize::<AdvisorCampaignProduct>() {
        let record = record?;
        products.insert(record.id_product_restaurant);
    }

    Ok(products)
}

// Write calculated rankings into the output file
pub fn write_rankings(file_path: &str, rankings: &[Ranking]) -> anyhow::Result<()> {
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
