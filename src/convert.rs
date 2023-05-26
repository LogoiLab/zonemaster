use base64::{engine::general_purpose, Engine as _};
use rand::seq::SliceRandom;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};

pub struct ResponseData {
    pub domain: String,
    pub ip_addr: Option<String>,
    pub port: Option<u16>,
    pub success: bool,
    pub date: Option<String>,
    pub status: Option<i16>,
    pub resulting_url: Option<String>,
    pub server: Option<String>,
    pub content_security_policy: Option<String>,
    pub content_type: Option<String>,
    pub body: Option<String>,
}

pub fn to_queue<R: Read>(reader: BufReader<R>) -> VecDeque<String> {
    let mut list: Vec<String> = Vec::new();
    for line in reader.lines() {
        match line {
            Ok(o) => match o.split(".\t").next() {
                Some(s) => list.push(s.trim().into()),
                None => {
                    continue;
                }
            },
            Err(_e) => {
                eprintln!("Failed to read line from buffer: {}", _e);
                continue;
            }
        }
    }
    println!("Deduplicating domains...");
    list.dedup();
    println!("Randomizing work queue...");
    let mut rng = rand::thread_rng();
    list.shuffle(&mut rng);
    let mut queue: VecDeque<String> = VecDeque::new();
    for domain in list {
        queue.push_back(domain);
    }
    println!("Starting scan....");
    return queue;
}

pub async fn to_respdata(res: reqwest::Response, domain: String) -> ResponseData {
    let (ip, port) = match res.remote_addr() {
        Some(s) => (Some(format!("{}", s.ip())), Some(s.port())),
        None => (None, None),
    };
    let status: Option<i16> = Some(res.status().as_u16() as i16);
    let resulting_url: Option<String> = Some(String::from(res.url().as_str()).replace("\0", ""));
    let date = match res.headers().get("date") {
        Some(s) => Some(String::from(s.to_str().unwrap_or_default()).replace("\0", "")),
        None => None,
    };
    let server = match res.headers().get("server") {
        Some(s) => Some(String::from(s.to_str().unwrap_or_default()).replace("\0", "")),
        None => None,
    };
    let content_security_policy = match res.headers().get("content-security-policy") {
        Some(s) => Some(String::from(s.to_str().unwrap_or_default()).replace("\0", "")),
        None => None,
    };
    let content_type = match res.headers().get("content-type") {
        Some(s) => Some(String::from(s.to_str().unwrap_or_default()).replace("\0", "")),
        None => None,
    };
    let body = match res.text().await {
        Ok(o) => Some(general_purpose::STANDARD_NO_PAD.encode(o)),
        Err(_) => None,
    };

    return ResponseData {
        domain: domain,
        ip_addr: ip,
        port: port,
        success: true,
        date,
        status,
        resulting_url,
        server,
        content_security_policy,
        content_type,
        body,
    };
}
