use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use crate::{error::ServerError, server::{init::{HandleHashOp, HandleListOp, HandleSetOp, HandleStringOp}, redis::RedisServer}};





// pub trait transaction{
//     fn HandleTransactions(&self,t:String);
// }

// impl transaction for RedisServer{

 
    
// }


pub async fn HandleTransactions(redis: &mut MutexGuard<'_, RedisServer>, transaction_queue:String)->Result<Option<Vec<u8>>, ServerError> {
    let mut responses: Vec<Result<Option<Vec<u8>>, ServerError>> = Vec::new();

    for command in transaction_queue.lines() {
        
        let (t, operation) = match command.split_once(' ') {
            Some((first, rest)) => (first.to_string(), rest.to_string()),
            None => (command.to_string(), String::new()),
        };

        
        let response = match t.trim() {
            "string" => {
                HandleStringOp(
                    redis,
                    operation.to_string(),
                    String::from("string"),
                ).await
            }

            "list" => {
                HandleListOp(
                    redis,
                    operation.to_string(),
                    String::from("list"),
                ).await
            }

            "hash" => {
                HandleHashOp(
                    redis,
                    operation.to_string(),
                    String::from("hash"),
                ).await
            }

            "set" => {
                HandleSetOp(
                    redis,
                    operation.to_string(),
                    String::from("set"),
                ).await
            }

            _ => {
                Err(ServerError::InvalidRedisType(
                    format!("Invalid Redis type in transaction: {}", t)
                ))
            }
        };
        responses.push(response);
    }

    let mut result = Vec::new();

    for response in responses {
        match response {
            Ok(Some(res)) => {
                result.extend_from_slice(&res);
                result.push(b'\n');
            }

            Ok(None) => {
                result.extend_from_slice(b"OK");
                result.push(b'\n');
            }

            Err(e) => {
                result.extend_from_slice(e.to_string().as_bytes());
                result.push(b'\n');
            }
        }
    }

    Ok(Some(result))
}