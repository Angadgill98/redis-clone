use std::{sync::{Arc}};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::OwnedReadHalf}, sync::{Mutex, MutexGuard, oneshot::Sender},
};

use crate::{
    error::ServerError, server::{persistenc::Persistence, pubSub, redis::{
        self, RedisHash, RedisList, RedisServer, RedisSet, RedisString, RedisValue,
    }, transactions},
};

pub async fn Init(sender: Sender<u8>) -> Result<(), ServerError> {
    let socket = CreateSocket().await?;

    sender
        .send(1)
        .map_err(|_| ServerError::InvalidRedisCommand(
            "Failed to notify server startup".to_string()
        ))?;

    
    let redis = Arc::new(Mutex::new(RedisServer::new()));
    
    let mut reconstructor_redis=redis.lock().await;
    //for reconstruction from log file
    // let content=reconstructor_redis.ReadLog();
    // let mpa=reconstructor_redis.ReconstructLogFile(content);

    // reconstructor_redis.data=mpa;


    //for reconstruction from snapshots
    // reconstructor_redis.SaveSnapShot()?;

    // reconstructor_redis.ReadSnapShot()?;

    drop(reconstructor_redis);

    println!("Server: running");

    loop {
        let (mut stream, client_addr) = socket.accept().await?;
        let (mut reader,writer)=stream.into_split();
        let redis_thread = Arc::clone(&redis);
        let mut redis=redis_thread.lock().await;
        redis.Clients.insert(client_addr, writer);

        drop(redis);
        tokio::spawn(async move {
            if let Err(e) = HandleClient(redis_thread,&mut reader, client_addr).await {
                
            }
        });
    }
}

async fn CreateSocket() -> Result<TcpListener, ServerError> {
    let addr = std::env::var("redis_server_addr")?;

    let socket = TcpListener::bind(addr).await?;

    Ok(socket)
}

async fn HandleClient(redis: Arc<Mutex<RedisServer>>,stream:&mut OwnedReadHalf,client_addr: core::net::SocketAddr) -> Result<(), ServerError> {
    loop {
        let mut buf_len = [0u8; 8];

        stream.read_exact(&mut buf_len).await?;

        let len = u64::from_be_bytes(buf_len) as usize;

        let mut buf = vec![0u8; len];

        stream.read_exact(&mut buf).await?;

        // println!("Server: {:?},{}",buf,len);

        let (redis_type, command) = Simplify(&buf)?;
        
        match HandleType(&redis, redis_type.clone(), command.clone(),client_addr.clone()).await{
       
            Ok(Some(res)) => {
                let mut redis = redis.lock().await;

                redis.WriteToLog(command.clone());

                let writer = redis.Clients.get_mut(&client_addr).unwrap();

                let mut response = Vec::with_capacity(1 + 8 + res.len());

                response.push(1);

                let response_len = (res.len() as u64).to_be_bytes();
                response.extend_from_slice(&response_len);

                response.extend_from_slice(&res);

                writer.write_all(&response).await?;

                println!("Server: Operation was successful, sending response");

                drop(redis);
            }

            Ok(None) => {
                if redis_type.trim()=="pubSub" {
                    
                }else{
                    let mut redis = redis.lock().await;

                    redis.WriteToLog(command.clone());

                    let writer = redis.Clients.get_mut(&client_addr).unwrap();

                    let response = [1u8];

                    writer.write_all(&response).await?;

                    println!("Server: Operation was successful, no response data");

                    drop(redis);
                }
                
            }

            Err(e) => {
                eprintln!("Client error: {}", e);

                let mut redis = redis.lock().await;

                let writer = redis.Clients.get_mut(&client_addr).unwrap();

                let err = e.to_string();
                let err_bytes = err.as_bytes();


                let mut response = Vec::with_capacity(1 + 8 + err_bytes.len());

                response.push(0);

                let error_len = (err_bytes.len() as u64).to_be_bytes();
                response.extend_from_slice(&error_len);

                response.extend_from_slice(err_bytes);

                writer.write_all(&response).await?;

                drop(redis);
            }
        }       
        
        
        {
        let redis=redis.lock().await;
        println!("Server: the map struct is {:?}",redis.data);
        println!("Server: connected clients {:?}",redis.Clients);
        println!("Server: channels {:?}",redis.Channels);
        }   
    }
}

fn Simplify(operation: &[u8]) -> Result<(String, String), ServerError> {
    let mut position = 0;

    if operation.len() < 8 {
        return Err(ServerError::InvalidRedisCommand(
            "Invalid operation: missing type length".to_string(),
        ));
    }

    let type_len = u64::from_be_bytes(
        operation[position..position + 8]
            .try_into()
            .map_err(|_| ServerError::InvalidRedisCommand(
                "Invalid type length".to_string()
            ))?,
    ) as usize;

    position += 8;

    if position + type_len > operation.len() {
        return Err(ServerError::InvalidRedisCommand(
            "Invalid operation: type length exceeds packet size".to_string(),
        ));
    }

    let redis_type = String::from_utf8(
        operation[position..position + type_len].to_vec(),
    )
    .map_err(|_| ServerError::InvalidRedisCommand(
        "Redis type contains invalid UTF-8".to_string(),
    ))?;

    position += type_len;

    if position + 8 > operation.len() {
        return Err(ServerError::InvalidRedisCommand(
            "Invalid operation: missing command length".to_string(),
        ));
    }

    let command_len = u64::from_be_bytes(
        operation[position..position + 8]
            .try_into()
            .map_err(|_| ServerError::InvalidRedisCommand(
                "Invalid command length".to_string()
            ))?,
    ) as usize;

    position += 8;

    if position + command_len > operation.len() {
        return Err(ServerError::InvalidRedisCommand(
            "Invalid operation: command length exceeds packet size".to_string(),
        ));
    }

    let command = String::from_utf8(
        operation[position..position + command_len].to_vec(),
    )
    .map_err(|_| ServerError::InvalidRedisCommand(
        "Command contains invalid UTF-8".to_string(),
    ))?;

    Ok((redis_type, command))
}

async fn HandleType(redis: &Arc<Mutex<RedisServer>>,redis_type: String,command: String,client_addr: core::net::SocketAddr) -> Result<Option<Vec<u8>>, ServerError> {
    let mut redis:MutexGuard<'_, RedisServer>=redis.lock().await;
    match redis_type.trim() {
        "string" => {
            HandleStringOp(&mut redis, command, String::from("string")).await
        }

        "list" => {
            HandleListOp(&mut redis, command, String::from("list")).await
        }

        "hash" => {
            HandleHashOp(&mut redis, command, String::from("hash")).await
        }

        "set" => {
            HandleSetOp(&mut redis, command, String::from("set")).await
        }
        "transaction"=>{
            transactions::HandleTransactions(&mut redis, command).await
        }
        "pubSub"=>{
            pubSub::HandlePubSub(&mut redis, command,client_addr).await

            
        }
       

        _ => {
            return Err(ServerError::InvalidRedisType(
                format!(
                    "Invalid Redis type: {}. Valid types: string, list, hash, set",
                    redis_type
                ),
            ));
        }
    }

    
}

pub async fn HandleStringOp(redis: &mut MutexGuard<'_, RedisServer>,command: String,t: String) -> Result<Option<Vec<u8>>, ServerError> {
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

    match command_name {
        "set" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "SET requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            

            redis.create_string(key, value);

            Ok(None)
        }

        "get" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "GET requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_string = GetRedisStingRef(redis_value)?;

            let value = redis_string.get().to_vec();

            Ok(Some(value))
        }

        "append" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "APPEND requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let append = command.remove(1);

            
            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_string = GetRedisSting(redis_value)?;

            redis_string.append(&append);

            Ok(None)
        }

        "len" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "LEN requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_string = GetRedisStingRef(redis_value)?;

            let len = (redis_string.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("Command {} not recognized", command_name),
            ))
        }
    }
    
}

fn GetRedisSting(value: &mut RedisValue) -> Result<&mut RedisString, ServerError> {
    match value {
        RedisValue::String(string) => Ok(string),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a string".to_string(),
        )),
    }
}

fn GetRedisStingRef(value: &RedisValue) -> Result<&RedisString, ServerError> {
    match value {
        RedisValue::String(string) => Ok(string),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a string".to_string(),
        )),
    }
}

pub async fn HandleListOp(redis: &mut MutexGuard<'_, RedisServer>,command: String,t: String) -> Result<Option<Vec<u8>>, ServerError> {
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
        .map_err(|_| ServerError::InvalidRedisCommand(
            "Redis command contains invalid UTF-8".to_string(),
        ))?;

    match command_name {
        "lcreate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "LCREATE requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            redis.create_list(key);

            Ok(None)
        }

        "lpush" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "LPUSH requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            redis_list.push_front(value);

            Ok(None)
        }

        "rpush" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "RPUSH requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            redis_list.push_back(value);

            Ok(None)
        }

        "lpop" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "LPOP requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            match redis_list.pop_front() {
                Some(value) => Ok(Some(value)),
                None => Ok(None),
            }
        }

        "rpop" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "RPOP requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            match redis_list.pop_back() {
                Some(value) => Ok(Some(value)),
                None => Ok(None),
            }
        }

        "llen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "LLEN requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            let len = (redis_list.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        "lindex" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "LINDEX requires a key and index".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let index: usize = String::from_utf8(value)
                .map_err(|_| ServerError::InvalidRedisCommand(
                    "LINDEX index contains invalid UTF-8".to_string(),
                ))?
                .parse()
                .map_err(|_| ServerError::InvalidRedisCommand(
                    "LINDEX index must be a valid number".to_string(),
                ))?;

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            let element = redis_list
                .get(index)
                .ok_or_else(|| ServerError::InvalidRedisCommand(
                    "List index out of bounds".to_string(),
                ))?;

            Ok(Some(element.clone()))
        }

        "lset" => {
            if command.len() < 4 {
                return Err(ServerError::InvalidRedisCommand(
                    "LSET requires a key, index and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value_index = command.remove(1);
            let new_value = command.remove(1);

            let index: usize = String::from_utf8(value_index)
                .map_err(|_| ServerError::InvalidRedisCommand(
                    "LSET index contains invalid UTF-8".to_string(),
                ))?
                .parse()
                .map_err(|_| ServerError::InvalidRedisCommand(
                    "LSET index must be a valid number".to_string(),
                ))?;

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            if index >= redis_list.len() {
                return Err(ServerError::InvalidRedisCommand(
                    "List index out of bounds".to_string(),
                ));
            }

            redis_list.set(index, new_value);

            Ok(None)
        }

        "lclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "LCLEAR requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string()
                ))?;

            let redis_list = GetRedisList(redis_value)?;

            redis_list.clear();

            Ok(None)
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("Command {} not recognized", command_name),
            ))
        }
    }
}

fn GetRedisList(value: &mut RedisValue) -> Result<&mut RedisList, ServerError> {
    match value {
        RedisValue::List(list) => Ok(list),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a list".to_string(),
        )),
    }
}

pub async fn HandleHashOp(redis: &mut MutexGuard<'_, RedisServer>,command: String,t: String) -> Result<Option<Vec<u8>>, ServerError> {
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

    match command_name {
        "hcreate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "HCREATE requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            redis.create_hash(key);

            Ok(None)
        }

        "hset" => {
            if command.len() < 4 {
                return Err(ServerError::InvalidRedisCommand(
                    "HSET requires a key, field and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);
            let value = command.remove(1);

            

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHash(redis_value)?;

            redis_hash.set(field, value);

            Ok(None)
        }

        "hget" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "HGET requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHashRef(redis_value)?;

            let value = redis_hash
                .get(&field)
                .ok_or_else(|| {
                    ServerError::InvalidRedisCommand(
                        "Hash field does not exist".to_string(),
                    )
                })?;

            Ok(Some(value.to_vec()))
        }

        "hexists" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "HEXISTS requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            
                ;

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHashRef(redis_value)?;

            let exists = redis_hash.exists(&field);

            Ok(Some(vec![exists as u8]))
        }

        "hdel" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "HDEL requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHash(redis_value)?;

            redis_hash.remove(&field);

            Ok(None)
        }

        "hlen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "HLEN requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHashRef(redis_value)?;

            let len = (redis_hash.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        "hclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "HCLEAR requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHash(redis_value)?;

            redis_hash.clear();

            Ok(None)
        }

        "hkeys" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "HKEYS requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHashRef(redis_value)?;

            let keys = redis_hash.keys();

            let mut result = Vec::new();

            for key in keys {
                let len = (key.len() as u64).to_be_bytes();
                result.extend_from_slice(&len);
                result.extend_from_slice(&key);
            }

            Ok(Some(result))
        }

        "hvalues" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "HVALUES requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| {
                    ServerError::NoRedisKey(
                        String::from_utf8_lossy(&key).to_string(),
                    )
                })?;

            let redis_hash = GetRedisHashRef(redis_value)?;

            let values = redis_hash.values();

            let mut result = Vec::new();

            for value in values {
                let len = (value.len() as u64).to_be_bytes();
                result.extend_from_slice(&len);
                result.extend_from_slice(&value);
            }

            Ok(Some(result))
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("Command {} not recognized", command_name),
            ))
        }
    }
}

fn GetRedisHash(value: &mut RedisValue) -> Result<&mut RedisHash, ServerError> {
    match value {
        RedisValue::Hash(hash) => Ok(hash),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a hash".to_string(),
        )),
    }
}

fn GetRedisHashRef(value: &RedisValue) -> Result<&RedisHash, ServerError> {
    match value {
        RedisValue::Hash(hash) => Ok(hash),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a hash".to_string(),
        )),
    }
}

pub async fn HandleSetOp(redis: &mut MutexGuard<'_, RedisServer>,command: String,t: String) -> Result<Option<Vec<u8>>, ServerError> {
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
        .map_err(|_| ServerError::InvalidRedisCommand(
            "Redis command contains invalid UTF-8".to_string(),
        ))?;

    match command_name {
        "screate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "SCREATE requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            redis.create_set(key);

            Ok(None)
        }

        "sadd" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "SADD requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSet(redis_value)?;

            redis_set.add(value);

            Ok(None)
        }

        "srem" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "SREM requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSet(redis_value)?;

            redis_set.remove(&value);

            Ok(None)
        }

        "scontains" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "SCONTAINS requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            
                

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSetRef(redis_value)?;

            let contains = redis_set.contains(&value);

            // 1 = true, 0 = false
            Ok(Some(vec![contains as u8]))
        }

        "slen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "SLEN requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSetRef(redis_value)?;

            let len = (redis_set.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        "sclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "SCLEAR requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get_mut(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSet(redis_value)?;

            redis_set.clear();

            Ok(None)
        }

        "svalues" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "SVALUES requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            
                

            let redis_value = redis.data
                .get(&key)
                .ok_or_else(|| ServerError::NoRedisKey(
                    String::from_utf8_lossy(&key).to_string(),
                ))?;

            let redis_set = GetRedisSetRef(redis_value)?;

            let values = redis_set.values();

            // Serialize:
            // [number of values][value length][value][value length][value]...
            let mut result = Vec::new();

            result.extend_from_slice(
                &(values.len() as u64).to_be_bytes()
            );

            for value in values {
                result.extend_from_slice(
                    &(value.len() as u64).to_be_bytes()
                );

                result.extend_from_slice(&value);
            }

            Ok(Some(result))
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("Command {} not recognized", command_name),
            ))
        }
    }
}

fn GetRedisSet(value: &mut RedisValue) -> Result<&mut RedisSet, ServerError> {
    match value {
        RedisValue::Set(set) => Ok(set),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a set".to_string(),
        )),
    }
}

fn GetRedisSetRef(value: &RedisValue) -> Result<&RedisSet, ServerError> {
    match value {
        RedisValue::Set(set) => Ok(set),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a set".to_string(),
        )),
    }
}
