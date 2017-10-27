#[macro_use]
extern crate log;
extern crate env_logger;
extern crate chrono;
#[macro_use]
extern crate mysql;
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
use std::error::Error;

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

#[derive(Debug)]
struct Goods {
    id: String,
    comment_num: u32,
}

fn get_goods_count(pool: &mysql::Pool) -> u32 {
    let count: Option<u32> = pool.first_exec(r"SELECT count(*) FROM jd_goods", ())
        .map(|result| {
            result.map(|x| x.unwrap()).map(|mut row| {
                mysql::from_value::<u32>(row.pop().unwrap())
            })
        }).unwrap();

    return count.unwrap();
}

fn get_goods_by_page(pool: &mysql::Pool, current_size: u32, page_size: u32) -> Vec<Goods> {
    let selected_goods: Vec<Goods> = pool.prep_exec(r"SELECT * FROM jd_goods LIMIT :current_size, :page_size", params! {
        "current_size" => current_size,
        "page_size" => page_size,
    }).map(|result| {
        result.map(|x| x.unwrap()).map(|mut row| {
            let id: String = row.take("ID").unwrap();
            Goods {
                id: id.to_string(),
                comment_num: row.take("comment_num").unwrap(),
            }
        }).collect()
    }).unwrap();

    return selected_goods;
}

fn crawl(product_id: &str, page: u32) -> Result<String, Box<Error>> {
    let mut core = Core::new()
        .expect("Failed to initialize core");
    let handle = core.handle();
    let client = hyper::Client::configure()
        .connector(HttpsConnector::new(4, &handle).unwrap())
        .build(&handle);

    // let url = format!("https://club.jd.com/comment/skuProductPageComments.action?productId={}&score=0&sortType=6&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1", product_id, page);
    // let url = format!("https://club.jd.com/comment/productPageComments.action?productId={}&score=0&sortType=6&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1", product_id, page);
    let url = format!("https://club.jd.com/productpage/p-{}-s-0-t-6-p-{}.html", product_id, page);
    let uri = try!(url.parse());
    let work = client.get(uri).and_then(|res| {
        res.body().concat2()
    });
    let got = try!(core.run(work));
    let decode_got = GBK.decode(&got, DecoderTrap::Strict)
        .expect("Failed to decode from gbk");
    let encode_got = UTF_8.encode(&decode_got, EncoderTrap::Strict)
        .expect("Failed to encode to utf8");
    let content = str::from_utf8(&encode_got)
        .expect("Failed to parse bytes to string");
    Ok(content.to_string())
}

fn process(content: &str, collection: &mongodb::coll::Collection) -> Result<usize, Box<Error>> {
    let json: Value = try!(serde_json::from_str(content));
    Ok(json.get("comments").map_or(0, |comments| {
        Bson::from(comments.clone()).as_array().map_or(0, |comments| {
            for comment in comments {
                let doc = comment.as_document().unwrap();
                let id = doc.get("id").unwrap();
                let mut doc_with_id = doc.clone();
                doc_with_id.insert("_id", Bson::from(id.clone()));
                collection.insert_one(doc_with_id, None)
                    .expect("Failed to insert document.");
            }
            comments.len()
        })
    }))
}

fn initialize_logger() -> Result<(), log::SetLoggerError> {
    env_logger::LogBuilder::new()
        .format(|record| {
            format!("{} [{}] - {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.args())
        })
        .target(env_logger::LogTarget::Stdout)
        .init()?;
    Ok(())
}

fn main() {
    initialize_logger()
        .expect("Failed to initialize logger");

    let settings = Settings::new()
        .expect("Failed to initialize settings");

    let pool = mysql::Pool::new(settings.mysql.url)
        .expect("Failed to connect to mysql database.");

    let client = mongodb::Client::connect(&settings.mongodb.host, settings.mongodb.port)
        .expect("Failed to initialize standalone client.");
    let collection = client.db(&settings.mongodb.db)
        .collection(&settings.mongodb.collection);

    let goods_count = get_goods_count(&pool);
    // let goods_count = 10;
    let page_size = settings.app.page_size;

    let mut current_size = 0u32;
    loop {
        let goods = get_goods_by_page(&pool, current_size, page_size);

        for x in goods.iter() {
            let mut comments_page = 0u32;
            let mut count = 0u32;
            loop {
                let n = match crawl(&x.id, comments_page) {
                    Ok(content) => match process(&content, &collection) {
                        Ok(n) => n,
                        Err(e) => {
                            error!("{}", e);
                            0
                        },
                    },
                    Err(e) => {
                        error!("{}", e);
                        0
                    },
                };

                comments_page += 1;
                count += n as u32;

                if n == 0 {
                    break;
                }
            }
            if count < x.comment_num {
                warn!("Product {}: {} comments expected, {} found.", x.id, x.comment_num, count);
            }
        }

        current_size += page_size;

        info!("Current Size: {}", current_size);

        if current_size > goods_count {
            break;
        }
    }
}
