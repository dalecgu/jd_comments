use config::{ConfigError, Config, File};

#[derive(Debug, Deserialize)]
pub struct MySQL {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct MongoDB {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub collection: String,
}

#[derive(Debug, Deserialize)]
pub struct App {
    pub page_size: u32,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub mysql: MySQL,
    pub mongodb: MongoDB,
    pub app: App,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name("config/default"))?;

        s.try_into()
    }
}
