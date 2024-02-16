use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use csv::{ReaderBuilder, WriterBuilder};
use std::{collections::HashMap, fs::File};

use model::{
    Ranking, SerializableRanking, User, ViewAction, ViewIdentifier, ViewRecord, ViewScope,
};

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

fn process_csv(file_path: &str) -> Result<HashMap<User, HashMap<ViewScope, Duration>>> {
    // We keep the time of last OPEN action for each (user, scope)
    let mut open_times: HashMap<ViewIdentifier, DateTime<Utc>> = HashMap::new();
    let mut views: UserViews = HashMap::new();

    // Open the csv file and setup the reader
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
        process_csv("data/product_views.csv").expect("to process product csv")
    });
    let product_image_proc_handle = std::thread::spawn(|| {
        process_csv("data/product_image_views.csv").expect("to process product_image csv")
    });
    let category_proc_handle = std::thread::spawn(|| {
        process_csv("data/category_views.csv").expect("to process category csv")
    });

    // Wait for the threads to finish calculating...
    let (product_views, product_image_views, category_views) = (
        product_proc_handle.join().expect("to process products"),
        product_image_proc_handle
            .join()
            .expect("to process product images"),
        category_proc_handle.join().expect("to process categories"),
    );

    let mut rankings: Vec<Ranking> = Vec::with_capacity(100_000);

    // Iterate on users...
    for (user, product_views) in product_views.iter() {
        let empty_map: HashMap<ViewScope, Duration> = HashMap::new();
        let image_views = product_image_views.get(user).unwrap_or(&empty_map);
        let category_views = category_views.get(user).unwrap_or(&empty_map);

        // Iterate on the products views of that user...
        for (product, &product_view) in product_views.iter() {
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

            // Calculate the ranking
            let rank = 5 * image_view.num_seconds()
                + 1 * product_view.num_seconds()
                + (0.1 * (category_view.num_seconds()) as f32) as i64;

            let rank = rank as usize;

            rankings.push(Ranking {
                user: user.clone(),
                product_id: product.product_id.clone().expect("to have product_id"),
                rank,
            })
        }
    }

    // Write calculated rankings into the output file
    write_rankings("data/out.csv", &rankings).unwrap();

    println!("Done in {}ms", (Utc::now() - start).num_milliseconds());
}
