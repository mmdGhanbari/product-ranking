// // Import necessary crates and modules
// use chrono::{DateTime, Duration, Utc};
// use csv::ReaderBuilder;
// use serde::Deserialize;
// use std::collections::HashMap;
// use std::error::Error;
// use std::fs::File;

// // Define a struct to deserialize each record of the CSV files
// #[derive(Debug, Deserialize)]
// struct Record {
//     id: String,
//     action: String,
//     product_id: String,
//     mac_address: String,
//     date_insert: String,
//     #[serde(default)] // Mark the category field as optional, providing a default if missing
//     category: String,
// }

// // Function to process CSV files and calculate time spent on product/category pages
// fn process_csv(file_path: &str, is_category: bool) -> Result<HashMap<String, i64>, Box<dyn Error>> {
//     // Open the CSV file
//     let file = File::open(file_path)?;
//     // Create a CSV reader with headers enabled
//     let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
//     // Initialize a HashMap to store total times spent
//     let mut times: HashMap<String, i64> = HashMap::new();
//     // HashMap to keep track of open times without a corresponding close
//     let mut open_times: HashMap<(String, String), DateTime<Utc>> = HashMap::new();

//     // Iterate over each record in the CSV
//     for result in rdr.deserialize() {
//         let record: Record = result?;
//         // Parse the date_insert field into a DateTime object
//         let date_time: DateTime<Utc> = record.date_insert.parse()?;

//         match record.action.as_str() {
//             // If the action is OPEN, store the open time
//             "OPEN" => {
//                 open_times.insert(
//                     (record.product_id.clone(), record.mac_address.clone()),
//                     date_time,
//                 );
//             }
//             // If the action is CLOSE, calculate the time spent and update the total time
//             "CLOSE" => {
//                 if let Some(open_time) =
//                     open_times.remove(&(record.product_id.clone(), record.mac_address.clone()))
//                 {
//                     let duration = date_time.signed_duration_since(open_time).num_seconds();
//                     let key = if is_category {
//                         record.category.clone()
//                     } else {
//                         record.product_id.clone()
//                     };
//                     *times.entry(key).or_insert(0) += duration;
//                 }
//             }
//             _ => {}
//         }
//     }

//     // Handle open times without corresponding close times by capping at 30 seconds
//     for ((product_id, _), open_time) in open_times {
//         let now = Utc::now();
//         let duration = std::cmp::min(30, now.signed_duration_since(open_time).num_seconds());
//         let key = if is_category {
//             product_id.clone()
//         } else {
//             product_id
//         };
//         *times.entry(key).or_insert(0) += duration;
//     }

//     Ok(times)
// }

// fn mainnnn() -> Result<(), Box<dyn Error>> {
//     // Process each CSV file and calculate total times
//     let product_view_times = process_csv("path_to_product_view.csv", false)?;
//     let product_image_view_times = process_csv("path_to_product_image_view.csv", false)?;
//     let category_view_times = process_csv("path_to_category_view.csv", true)?;

//     // Initialize a HashMap to store the ranking of each product
//     let mut product_rankings: HashMap<String, f64> = HashMap::new();

//     // Calculate rankings based on the provided formula
//     for (product_id, time) in &product_view_times {
//         let image_time = product_image_view_times.get(product_id).unwrap_or(&0);
//         let category_time = category_view_times.get(product_id).unwrap_or(&0); // Assuming product_id for category lookup, adjust if necessary

//         let ranking = 5.0 * (*image_time as f64)
//             + 1.0 * (*time as f64)
//             + (1.0 / 10.0) * (*category_time as f64);
//         product_rankings.insert(product_id.clone(), ranking);
//     }

//     // Sort products by their calculated rankings
//     let mut sorted_products: Vec<(String, f64)> = product_rankings.into_iter().collect();
//     sorted_products.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

//     // Print sorted products with their rankings
//     for (product_id, ranking) in sorted_products {
//         println!("Product ID: {}, Ranking: {}", product_id, ranking);
//     }

//     Ok(())
// }
