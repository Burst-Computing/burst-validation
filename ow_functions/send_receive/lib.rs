use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    #[serde(default = "stranger")]
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    body: String,
}

fn stranger() -> String {
    "stranger".to_string()
}

pub fn main(args: Value, burst_middleware: MiddlewareActorHandle) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    if input.name == "0" {
        let msg = burst_middleware.recv(1).unwrap();
        assert_eq!(msg.data, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        return serde_json::to_value(Output {
            body: format!("Received message from function 0: {:?}", msg),
        });
    } else {
        match burst_middleware.send(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into()) {
            Ok(_) => {
                return serde_json::to_value(Output {
                    body: format!("Sent message to function 1"),
                });
            }
            Err(e) => {
                return serde_json::to_value(Output {
                    body: format!("Error sending message to function 1: {:?}", e),
                });
            }
        }
    }
}