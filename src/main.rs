extern crate encoding;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate tokio_core;
extern crate serde_json;
extern crate bson;
extern crate mongodb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate config;

mod settings;

use std::str;

use encoding::all::{GBK, UTF_8};
use encoding::{Encoding, EncoderTrap, DecoderTrap};
use hyper_tls::HttpsConnector;
use futures::{Future, Stream};
use tokio_core::reactor::Core;
use serde_json::Value;
use bson::{Bson};
use mongodb::{ThreadedClient};
use mongodb::db::ThreadedDatabase;

use settings::Settings;

fn crawl(product_id: &str, page: u32) -> String {
    let mut core = Core::new()
        .expect("Failed to initialize core");
    let handle = core.handle();
    let client = hyper::Client::configure()
        .connector(HttpsConnector::new(4, &handle).unwrap())
        .build(&handle);

    let url = format!("https://club.jd.com/comment/skuProductPageComments.action?productId={}&score=0&sortType=6&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1", product_id, page);
    let uri = url.parse()
        .expect("Failed to parse url");
    let work = client.get(uri).and_then(|res| {
        assert_eq!(hyper::StatusCode::Ok, res.status());
        res.body().concat2()
    });
    let got = core.run(work)
        .expect("Failed to crawl");
    let decode_got = GBK.decode(&got, DecoderTrap::Strict)
        .expect("Failed to decode from gbk");
    let encode_got = UTF_8.encode(&decode_got, EncoderTrap::Strict)
        .expect("Failed to encode to utf8");
    let content = str::from_utf8(&encode_got)
        .expect("Failed to parse bytes to string");
    content.to_string()
}

fn process(content: &str, collection: &mongodb::coll::Collection) {
    let json: Value = serde_json::from_str(content)
        .expect("Failed to deserialize content as json");
    let json_comments = json.get("comments")
        .expect("Failed to find 'comments' field");
    let bson_comments = Bson::from(json_comments.clone());
    for bson_comment in bson_comments.as_array().unwrap() {
        let doc = bson_comment.as_document().unwrap();
        collection.insert_one(doc.clone(), None)
            .expect("Failed to insert document.");
    }
}

fn main() {
    let settings = Settings::new()
        .expect("Failed to initialize settings");

    let client = mongodb::Client::connect(&settings.mongodb.host, settings.mongodb.port)
        .expect("Failed to initialize standalone client.");
    let collection = client.db(&settings.mongodb.db)
        .collection(&settings.mongodb.collection);

    let product_id = "2341892";
    for i in 0..369 {
        process(&crawl(product_id, i), &collection);
    }
}
