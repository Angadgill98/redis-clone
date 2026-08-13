use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use tokio::{io::AsyncWriteExt, sync::{MutexGuard, RwLock}};

use crate::{error::ServerError, server::redis::RedisServer};


impl RedisServer {
    async fn subscribe(&self,key:Vec<u8>,client_addr:SocketAddr){
        

        self.Channels
            .write().await
            .entry(key)
            .or_insert_with(|| RwLock::new(HashSet::new()))
            .write()
            .await
            .insert(client_addr);
    }
    
    
    async fn publish(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut addresses = match self.Channels.read().await.get(&key) {
            Some(addresses) => addresses.read().await.iter().copied().collect::<Vec<_>>(),
            None => return,
        };

        self.broadcast(&mut addresses,&value,&key).await;
    }
    async fn broadcast(&self,addresses: &mut Vec<SocketAddr>,value: &[u8],key: &Vec<u8>) {
        for addr in addresses{
            let result = match self.Clients.write().await.get_mut(&addr) {
                Some(writer) => {

                    let res_with_data = [1u8; 1];
                    let status = [1u8; 1];
                    let response_len = (value.len() as u64).to_be_bytes();

                    let mut buffer = Vec::with_capacity(
                        1 + 1 + 8 + value.len()
                    );

                    buffer.extend_from_slice(&res_with_data);
                    buffer.extend_from_slice(&status);
                    buffer.extend_from_slice(&response_len);
                    buffer.extend_from_slice(value);

                    writer.lock().await.write_all(&buffer).await.unwrap();
                }
                None => return,
            };
        }
    }
    
    async fn unsubscribe(&self,key:Vec<u8>,client_addr:SocketAddr){
        let mut guard=self.Channels.write().await;
        let ischannel=guard.get_mut(&key);
        match ischannel {
            Some(clients) => {
                // channel exists
                clients.write().await.remove(&client_addr);
            }

            None => {
                // channel doesn't exist
            }
        }
    }
}

pub async fn HandlePubSub(redis: Arc<RedisServer>,command:String,client_addr:SocketAddr)-> Result<Option<Vec<u8>>, ServerError> {
    let mut command: Vec<Vec<u8>> = command
        .split_whitespace()
        .map(|word| word.as_bytes().to_vec())
        .collect();

    if command.is_empty() {
        return Err(ServerError::InvalidRedisCommand(
            "No Redis command provided".to_string(),
        ));
    }

    let command_name = std::str::from_utf8(&command[0])
        .map_err(|_| {
            ServerError::InvalidRedisCommand(
                "Redis command contains invalid UTF-8".to_string(),
            )
        })?;

    
    match command_name.to_lowercase().as_str() {
        "subscribe" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "SUBSCRIBE requires a channel".to_string(),
                ));
            }

            let key = command.remove(1);
            redis.subscribe(key, client_addr);
        }

        "publish" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "PUBLISH requires a channel and message".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            redis.publish(key, value).await;
        }

        "unsubscribe" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "UNSUBSCRIBE requires a channel".to_string(),
                ));
            }

            let key = command.remove(1);
            redis.unsubscribe(key, client_addr).await;
        }

        _ => {
            return Err(ServerError::InvalidRedisCommand(
                "Unknown Pub/Sub command".to_string(),
            ));
        }
    }
    

    Ok(None)

}