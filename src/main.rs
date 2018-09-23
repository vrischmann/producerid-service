extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate rand;
extern crate redis;
extern crate tiny_http;

use redis::RedisError;
use serde_json::Error as JSONError;
use std::error::Error as ErrorTrait;
use std::io::Error as IOError;

use chrono::prelude::*;
use redis::Commands;
use std::collections::HashSet;
use std::fmt;

struct CustomError {
    description: String,
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

enum Error {
    IO(IOError),
    JSON(JSONError),
    Redis(RedisError),
    Other(CustomError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref e) => write!(f, "{}", e),
            Error::JSON(ref e) => write!(f, "{}", e),
            Error::Redis(ref e) => write!(f, "{}", e),
            Error::Other(ref e) => write!(f, "{}", e),
        }
    }
}

impl From<JSONError> for Error {
    fn from(err: JSONError) -> Self {
        return Error::JSON(err);
    }
}

impl From<IOError> for Error {
    fn from(err: IOError) -> Self {
        return Error::IO(err);
    }
}

impl From<RedisError> for Error {
    fn from(err: RedisError) -> Self {
        return Error::Redis(err);
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        return Error::Other(CustomError {
            description: err.description().to_owned(),
        });
    }
}

type ProducerID = u16;

#[derive(Debug, Serialize, Deserialize)]
struct PodHistoryEntry {
    producer_id: ProducerID,
    date: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProducerHistoryEntry {
    pod_name: String,
    date: DateTime<Utc>,
}

struct Processor {
    conn: redis::Connection,
}

type PodHistory = Vec<PodHistoryEntry>;
type ProducerHistory = Vec<ProducerHistoryEntry>;

impl Processor {
    fn new(client: redis::Client) -> Result<Processor, Error> {
        return Ok(Processor {
            conn: client.get_connection()?,
        });
    }

    const REDIS_IDS_KEY: &'static str = "producerid-service::ids";

    fn new_id(all_ids: HashSet<ProducerID>) -> ProducerID {
        let mut n: u16 = 0;
        let mut exists: bool = true;

        while n <= 0 || exists {
            n = rand::random::<u16>();
            exists = all_ids.contains(&n);
        }

        n
    }

    fn mk_pod_key(name: &str) -> String {
        const K: &'static str = "producerid-service::history_per_pod";
        format!("{}::{}", K, name)
    }

    fn mk_producer_key(id: ProducerID) -> String {
        const K: &'static str = "producerid-service::history_per_producer";
        format!("{}::{}", K, id)
    }

    fn pod_history(&mut self, pod_name: &str) -> Result<PodHistory, Error> {
        self.history(&Processor::mk_pod_key(pod_name))
    }

    fn producer_history(&mut self, producer_id: ProducerID) -> Result<ProducerHistory, Error> {
        self.history(&Processor::mk_producer_key(producer_id))
    }

    fn history<T: serde::de::DeserializeOwned>(&mut self, key: &str) -> Result<Vec<T>, Error> {
        let vals: Vec<String> = self.conn.lrange(key, 0, -1)?;

        let iter = vals.into_iter();
        let mapped: Result<Vec<_>, JSONError> = iter.map(|v| serde_json::from_str(&v)).collect();

        // Wut ?
        match mapped {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::JSON(e)),
        }
    }

    fn release(&mut self, pod_name: &str) -> Result<(), Error> {
        self.conn.hdel(Processor::REDIS_IDS_KEY, pod_name)?;
        Ok(())
    }

    fn acquire(&mut self, pod_name: &str) -> Result<ProducerID, Error> {
        let all_ids: HashSet<ProducerID> = self.conn.hvals(Processor::REDIS_IDS_KEY)?;

        let id = self
            .conn
            .hget(Processor::REDIS_IDS_KEY, pod_name)
            .or_else(|_| {
                let new_id = Processor::new_id(all_ids);
                Ok(new_id)
            });

        match id {
            Ok(v) => {
                self.conn.hset(Processor::REDIS_IDS_KEY, pod_name, v)?;

                self.conn.lpush(
                    Processor::mk_pod_key(pod_name),
                    serde_json::to_string(&PodHistoryEntry {
                        producer_id: v,
                        date: Utc::now(),
                    })?,
                )?;
                self.conn.lpush(
                    Processor::mk_producer_key(v),
                    serde_json::to_string(&ProducerHistoryEntry {
                        pod_name: pod_name.to_owned(),
                        date: Utc::now(),
                    })?,
                )?;

                id
            }
            _ => id,
        }
    }
}

struct Server {
    http_server: tiny_http::Server,
    processor: Processor,
}

impl Server {
    fn new() -> Result<Server, Error> {
        let http_server = tiny_http::Server::http("0.0.0.0:6070").unwrap();
        let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
        let processor = Processor::new(redis_client)?;

        return Ok(Server {
            http_server: http_server,
            processor: processor,
        });
    }

    fn process_one(&mut self, mut hreq: tiny_http::Request) -> Result<(), Error> {
        let response = match hreq.url().as_ref() {
            "/history/pod" => {
                #[derive(Deserialize)]
                struct Request {
                    pod_name: String,
                }

                let reader = hreq.as_reader();
                let r: Request = serde_json::from_reader(reader)?;

                let all_ids = self.processor.pod_history(&r.pod_name)?;

                let json = json!({
                    "producer_ids": all_ids,
                });

                json.to_string()
            }
            "/history/producer" => {
                #[derive(Deserialize)]
                struct Request {
                    producer_id: u16,
                }

                let reader = hreq.as_reader();
                let r: Request = serde_json::from_reader(reader)?;

                let all_pods = self.processor.producer_history(r.producer_id)?;

                let json = json!({
                    "pods": all_pods,
                });

                json.to_string()
            }
            "/acquire" => {
                #[derive(Deserialize)]
                struct Request {
                    pod_name: String,
                }

                let reader = hreq.as_reader();
                let r: Request = serde_json::from_reader(reader)?;

                if r.pod_name.is_empty() {}

                let producer_id = self.processor.acquire(&r.pod_name)?;

                let json = json!({
                    "producer_id": producer_id,
                });

                json.to_string()
            }
            "/release" => {
                #[derive(Deserialize)]
                struct Request {
                    pod_name: String,
                }

                let reader = hreq.as_reader();
                let r: Request = serde_json::from_reader(reader)?;

                self.processor.release(&r.pod_name)?;

                json!({"status": "OK"}).to_string()
            }
            _ => json!({"status": "OK"}).to_string(),
        };

        hreq.respond(tiny_http::Response::from_string(response))?;
        Ok(())
    }

    fn run(&mut self) -> Result<(), Error> {
        loop {
            let hreq = self.http_server.recv()?;

            if let Err(e) = self.process_one(hreq) {
                eprintln!("error: {}", e);
            }
        }
    }
}

fn run() -> Result<(), Error> {
    let mut server = Server::new()?;
    return server.run();
}

fn main() {
    ::std::process::exit(match run() {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {}", err);
            1
        }
    });
}
