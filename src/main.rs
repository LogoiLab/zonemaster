use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use dotenv::dotenv;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tokio_postgres::NoTls;

mod convert;

async fn initialize_database(dbpool: &Pool) {
    let dbclient = dbpool.get().await.unwrap();
    let stmt = dbclient
        .prepare_cached(
            "CREATE TABLE IF NOT EXISTS root_documents (
                    domain                  TEXT PRIMARY KEY,
                    ip_addr                 TEXT,
                    port                    SMALLINT,
                    req_num                 SERIAL,
                    success                 BOOL NOT NULL,
                    date                    TEXT,
                    status                  SMALLINT,
                    resulting_url           TEXT,
                    server                  TEXT,
                    content_security_policy TEXT,
                    content_type            TEXT,
                    body                    TEXT

            );",
        )
        .await
        .unwrap();
    dbclient.query(&stmt, &[]).await.unwrap();
}

async fn query_server(
    _worker_id: &usize,
    client: &reqwest::Client,
    domain: String,
) -> Option<reqwest::Response> {
    let url = format!("https://{}/", domain);
    match client.get(url).send().await {
        Ok(o) => return Some(o),
        Err(_e) => {
            //eprintln!("Worker[{}] Reqwest error {}", worker_id, _e);
            return None;
        }
    }
}

async fn store(
    _worker_id: &usize,
    dbpool: &Pool,
    domain: String,
    data: Option<convert::ResponseData>,
) {
    let dbclient = dbpool.get().await.unwrap();
    match data {
        Some(s) => {
            let stmt = dbclient.prepare_cached("INSERT INTO root_documents (domain, ip_addr, port, success, date, status, resulting_url, server, content_security_policy, content_type, body) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT DO NOTHING").await.unwrap();
            match dbclient
                .query(
                    &stmt,
                    &[
                        &domain,
                        &s.ip_addr,
                        &match s.port {
                            Some(s) => Some(s as i16),
                            None => None,
                        },
                        &s.success,
                        &s.date,
                        &s.status,
                        &s.resulting_url,
                        &s.server,
                        &s.content_security_policy,
                        &s.content_type,
                        &s.body,
                    ],
                )
                .await
            {
                Ok(_) => (),
                Err(_e) => {
                    eprintln!(
                        "Worker[{}]: Failed to insert success case: {}",
                        _worker_id, _e
                    );
                }
            }
        }
        None => {
            let stmt = dbclient
                .prepare_cached("INSERT INTO root_documents (domain, success) VALUES ($1, false) ON CONFLICT DO NOTHING")
                .await
                .unwrap();
            match dbclient.query(&stmt, &[&domain]).await {
                Ok(_) => (),
                Err(_e) => {
                    eprintln!("Worker[{}]: Failed to insert fail case: {}", _worker_id, _e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let worker_count = (num_cpus::get() * 2) - 1;
    dotenv().ok();

    let mut cfg = Config::new();
    cfg.dbname = Some(
        std::env::var("DB_NAME")
            .expect("Please include a DB_NAME var in your .env file.")
            .to_string(),
    );
    cfg.user = Some(
        std::env::var("DB_USER")
            .expect("Please include a DB_USER var in your .env file.")
            .to_string(),
    );
    cfg.password = Some(
        std::env::var("DB_PASS")
            .expect("Please include a DB_PASS var in your .env file.")
            .to_string(),
    );
    cfg.host = Some(
        std::env::var("DB_HOST")
            .expect("Please include a DB_HOST var in your .env file.")
            .to_string(),
    );
    cfg.port = Some(
        std::env::var("DB_PORT")
            .expect("Please include a DB_PORT var in your .env file.")
            .parse::<u16>()
            .unwrap_or(5432),
    );
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let dbpool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    initialize_database(&dbpool).await;
    let dbclient = dbpool.get().await.unwrap();
    let stmt = dbclient.prepare_cached("").await.unwrap();
    dbclient.query(&stmt, &[]).await.unwrap();

    let queue: Arc<Mutex<VecDeque<String>>> =
        Arc::new(Mutex::new(convert::to_queue(BufReader::new(
            File::open(
                std::env::args()
                    .nth(1)
                    .expect("You must provide an input file path."),
            )
            .expect("Failed to open specified input file."),
        ))));
    let init_queue_size = queue.lock().unwrap().len();
    for worker in 0..worker_count {
        let worker_queue = queue.clone();
        let worker_dbpool = dbpool.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .user_agent("Fuck UCEProtect.net, all my homies hate UCEProtect!")
                .timeout(std::time::Duration::from_secs(2))
                .danger_accept_invalid_certs(true)
                .build()
                .expect(format!("Worker[{}]: Failed to build reqwest client.", &worker).as_str());

            while !worker_queue.lock().unwrap().is_empty() {
                let task = worker_queue.lock().unwrap().pop_front();
                match task {
                    Some(s) => {
                        let res = query_server(&worker, &client, s.clone());
                        match res.await {
                            Some(res) => {
                                store(
                                    &worker,
                                    &worker_dbpool,
                                    s.clone(),
                                    Some(convert::to_respdata(res, s.clone()).await),
                                )
                                .await
                            }
                            None => store(&worker, &worker_dbpool, s, None).await,
                        }
                    }
                    None => continue,
                }
            }
        });
    }
    let progress = indicatif::ProgressBar::new(init_queue_size as u64);
    progress.set_style(
        indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:80.cyan/blue} {pos:>8}/{len:8}",
        )
        .unwrap(),
    );
    while queue.lock().unwrap().len() > 0 {
        progress.set_position((init_queue_size - queue.lock().unwrap().len()) as u64);
        sleep(Duration::from_millis(1000)).await;
    }
    println!("All tasks done.");
}
