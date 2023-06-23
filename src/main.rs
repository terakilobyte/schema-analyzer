use std::collections::HashMap;

use bson::{doc, Document};
use futures::stream::TryStreamExt;
use mongodb::{options::*, Client};

#[tokio::main]
async fn main() -> Result<(), ()> {
    println!("program entry");
    let start = std::time::Instant::now();
    let client_uri = "mongodb+srv://<username>:<password>@<cluster-url>";
    let client_options = ClientOptions::parse(client_uri).await.unwrap();
    let client = Client::with_options(client_options).unwrap();
    let database = client.database("<database>");
    let collection_name = "<collection>";

    println!("Initial setup: {:?}", start.elapsed());

    let document_count = database
        .collection::<Document>(collection_name)
        .estimated_document_count(None)
        .await
        .unwrap();

    let default_sample_size = 10000;

    // sample size is the max of the default sample size or the square root of the document count
    // it seems scientific enough
    let sample_size =
        f64::max(default_sample_size as f64, f64::sqrt(document_count as f64)).round() as i64;

    let pipeline = vec![
        doc! {
            "$sample": {
                "size": bson::Bson::Int64(sample_size)
            }
        },
        doc! {
            "$project": {
                "_id": 0,
                "schema": {
                    "$map": {
                        "input": {
                            "$objectToArray": "$$ROOT"
                        },
                        "as": "field",
                        "in": {
                            "k": "$$field.k",
                            "v": { "$type": "$$field.v" }
                        }
                    }
                }
            }
        },
        // group the documents, getting the keys and the schemas
        doc! {
            "$group": {
                "_id": null,
                "keys": {
                    "$addToSet": "$schema.k"
                },
                "schema": {
                    "$addToSet": "$schema"
                }
            }
        },
        // reduce the keys into a single array
        doc! {
            "$project": {
                "_id": 0,
                "keys": {
                    "$reduce": {
                        "input": "$keys",
                        "initialValue": [],
                        "in": {
                            "$setUnion": ["$$value", "$$this"]
                        }
                    }
                },
                "schema": 1
            }
        },
        // unwind the schema array
        doc! {
            "$unwind": "$schema"
        },
        // figure out which keys are missing from the schema. Insert them with the value "missing"
        doc! {
            "$project": {
                "schema": {
                    "$reduce": {
                        "input": {
                            "$setDifference": ["$keys", "$schema.k"]
                        },
                        "initialValue": "$schema",
                        "in": {
                            "$concatArrays": ["$$value", [{
                                "k": "$$this",
                                "v": "missing"
                            }]]
                        }
                    }
                }
            }
        },
        // group the documents again, converting the schemas back to objects and only keeping unique ones
        // why convert them back to objects? In testing, it seems to be faster
        {
            doc! {
                "$group": {
                    "_id": null,
                    "schema": {
                        "$addToSet":
                        {
                            "$arrayToObject": "$schema"
                        }
                    }
                }
            }
        },
        // unwind the schema array
        doc! {
            "$unwind": "$schema"
        },
        // project the schema object back to an array
        doc! {
            "$project": {
                "_id": 0,
                "schema": {
                    "$objectToArray": "$schema"
                }
            }
        },
        // unwind the schema array. We now have a document for each field and type
        doc! {
            "$unwind": "$schema"
        },
        // group by the key, adding unique values to the types array
        doc! {
            "$group": {
                "_id": "$schema.k",
                "types": {
                    "$addToSet": "$schema.v"
                }
            }
        },
        // group the groups into a single document
        doc! {
            "$group": {
                "_id": null,
                "schema": {
                    "$addToSet": {
                        "field": "$_id",
                        "types": "$types"
                    }
                }
            }
        },
    ];

    #[derive(Hash, Eq, PartialEq, Debug)]
    struct Schema {
        field: String,
        types: Vec<String>,
    }

    let mut schema = HashMap::<String, Vec<String>>::new();

    let pre_query = start.elapsed();

    println!("Pre-query: {:?}", pre_query);

    let mut result = database
        .collection::<Document>(collection_name)
        .aggregate(pipeline, None)
        .await
        .unwrap();

    let query = start.elapsed() - pre_query;

    println!("Query: {:?}", query);

    while let Some(doc) = result.try_next().await.unwrap() {
        // iterate over the schema entry, putting the fields and types into the map
        let schema_entry = doc.get("schema").unwrap().as_array().unwrap();
        schema_entry.into_iter().for_each(|entry| {
            let entry = entry.as_document().unwrap();
            let field = entry.get("field").unwrap().as_str().unwrap();
            let types = entry
                .get("types")
                .unwrap()
                .as_array()
                .unwrap()
                .into_iter()
                .map(|t| t.as_str().unwrap().to_string())
                .collect::<Vec<String>>();
            schema.insert(field.to_string(), types);
        });
        println!("{:?}", schema);
    }
    let post_query = start.elapsed() - pre_query - query;
    println!("Post-query: {:?}", post_query);

    println!("Total: {:?}", start.elapsed());

    Ok(())
}
