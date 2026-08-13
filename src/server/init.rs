use std::{sync::Arc, time::Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::OwnedReadHalf}, sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, oneshot::Sender},
};

use crate::{
    error::ServerError, server::{ persistenc, pubSub, redis::{
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

    
    let redis = Arc::new(RedisServer::new().await);
    
    // let mut reconstructor_redis=redis.data.write().await;;
    //for reconstruction from log file
    // let content=reconstructor_redis.ReadLog();
    // let mpa=reconstructor_redis.ReconstructLogFile(content);

    // reconstructor_redis.data=mpa;


    //for reconstruction from snapshots
    // reconstructor_redis.SaveSnapShot()?;

    // reconstructor_redis.ReadSnapShot()?;

    // drop(reconstructor_redis);

    println!("Server: running");

    loop {
        let (mut stream, client_addr) = socket.accept().await?;
        let (mut reader,writer)=stream.into_split();
        let redis_thread = Arc::clone(&redis);
        
        redis
        .Clients
        .write()
        .await
        .insert(client_addr, Arc::new(Mutex::new(writer)));

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

async fn HandleClient(redis: Arc<RedisServer>,stream:&mut OwnedReadHalf,client_addr: core::net::SocketAddr) -> Result<(), ServerError> {
    loop {
        let mut buf_len = [0u8; 8];

        stream.read_exact(&mut buf_len).await?;

        let len = u64::from_be_bytes(buf_len) as usize;

        let mut buf = vec![0u8; len];

        stream.read_exact(&mut buf).await?;

        // println!("Server: {:?},{}",buf,len);

        let (redis_type, command) = Simplify(&buf)?;
        
        match HandleType(Arc::clone(&redis), redis_type.clone(), command.clone(),client_addr.clone()).await{
       
            Ok(Some(res)) => {
                

                // persistenc::WriteToLog(command.clone());

                let writer = {
                    let writer_guard = redis.Clients.read().await;

                    writer_guard
                        .get(&client_addr)
                        .cloned()
                        .unwrap()
                }; // writer_guard is dropped here
                let mut response = Vec::with_capacity(1 + 8 + res.len());

                response.push(1);

                let response_len = (res.len() as u64).to_be_bytes();
                response.extend_from_slice(&response_len);

                response.extend_from_slice(&res);

                writer.lock()
                .await.write_all(&response).await?;

                // println!("Server: Operation was successful, sending response");

            }

            Ok(None) => {
                if redis_type.trim()=="pubSub" {
                    
                }else{
                   
                    // persistenc::WriteToLog(command.clone());

                    let writer = {
                        let writer_guard = redis.Clients.read().await;

                        writer_guard
                            .get(&client_addr)
                            .cloned()
                            .unwrap()
                    }; // writer_guard is dropped here
                    let response = [1u8];

                    writer.lock().await.write_all(&response).await?;

                    // println!("Server: Operation was successful, no response data");

                    
                }
                
            }

            Err(e) => {
                eprintln!("Client error: {}", e);

               
                let writer = {
                    let writer_guard = redis.Clients.read().await;

                    writer_guard
                        .get(&client_addr)
                        .cloned()
                        .unwrap()
                }; // writer_guard is dropped here

                let err = e.to_string();
                let err_bytes = err.as_bytes();


                let mut response = Vec::with_capacity(1 + 8 + err_bytes.len());

                response.push(0);

                let error_len = (err_bytes.len() as u64).to_be_bytes();
                response.extend_from_slice(&error_len);

                response.extend_from_slice(err_bytes);

                writer.lock().await.write_all(&response).await?;

                
            }
        }       
        
        
        // {
        
        // println!("Server: the map struct is {:?}",redis.data.read().await);
        // println!("Server: connected clients {:?}",redis.Clients.read().await);
        // println!("Server: channels {:?}",redis.Channels.read().await);
        // }   
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

async fn HandleType(redis: Arc<RedisServer>,redis_type: String,command: String,client_addr: core::net::SocketAddr) -> Result<Option<Vec<u8>>, ServerError> {
    
    match redis_type.trim() {
        "string" => {
            HandleStringOp(redis, command, String::from("string")).await
        }

        "list" => {
            HandleListOp(redis, command, String::from("list")).await
        }

        "hash" => {
            HandleHashOp( redis, command, String::from("hash")).await
        }

        "set" => {
            HandleSetOp( redis, command, String::from("set")).await
        }
        "transaction"=>{
            transactions::HandleTransactions(redis, command).await
        }
        "pubSub"=>{
            pubSub::HandlePubSub(redis, command,client_addr).await

            
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



// ============================================================
// Helper: get the RedisValue Arc without holding the map lock
// ============================================================

async fn get_value(
    redis: &Arc<RedisServer>,
    key: &[u8],
) -> Result<Arc<RwLock<RedisValue>>, ServerError> {
    let map_guard = redis.data.read().await;

    map_guard
        .get(key)
        .cloned()
        .ok_or_else(|| {
            ServerError::NoRedisKey(
                String::from_utf8_lossy(key).to_string(),
            )
        })
}


// ============================================================
// STRING
// ============================================================

pub async fn HandleStringOp(
    redis: Arc<RedisServer>,
    command: String,
    _t: String,
) -> Result<Option<Vec<u8>>, ServerError> {

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

        // ----------------------------------------------------
        // set key value
        // ----------------------------------------------------

        "set" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "set requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let command = format!(
                "set {} {}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );

            redis.create_string(key, value).await;

            redis.log_sender
            .send(command)
            .await
            .map_err(|_| {
                ServerError::InvalidRedisCommand(
                    "Log worker stopped".to_string()
                )
            })?;

           
            Ok(None)
        }

        // ----------------------------------------------------
        // get key
        // ----------------------------------------------------

        "get" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "get requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_string = GetRedisStringRef(&value_guard)?;

            Ok(Some(redis_string.get().to_vec()))
        }

        // ----------------------------------------------------
        // append key value
        // ----------------------------------------------------

        "append" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "append requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let append = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_string = GetRedisString(&mut value_guard)?;

            redis_string.append(&append);

            Ok(None)
        }

        // ----------------------------------------------------
        // len key
        // ----------------------------------------------------

        "len" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "len requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_string = GetRedisStringRef(&value_guard)?;

            let len = (redis_string.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("command {} not recognized", command_name),
            ))
        }
    }
}


// ============================================================
// LIST
// ============================================================

pub async fn HandleListOp(
    redis: Arc<RedisServer>,
    command: String,
    _t: String,
) -> Result<Option<Vec<u8>>, ServerError> {

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

        // ----------------------------------------------------
        // lcreate key
        // ----------------------------------------------------

        "lcreate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "lcreate requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            redis.create_list(key).await;

            Ok(None)
        }

        // ----------------------------------------------------
        // lpush key value
        // ----------------------------------------------------

        "lpush" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "lpush requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            redis_list.push_front(value);

            Ok(None)
        }

        // ----------------------------------------------------
        // rpush key value
        // ----------------------------------------------------

        "rpush" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "rpush requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            redis_list.push_back(value);

            Ok(None)
        }

        // ----------------------------------------------------
        // lpop key
        // ----------------------------------------------------

        "lpop" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "lpop requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            Ok(redis_list.pop_front())
        }

        // ----------------------------------------------------
        // rpop key
        // ----------------------------------------------------

        "rpop" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "rpop requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            Ok(redis_list.pop_back())
        }

        // ----------------------------------------------------
        // llen key
        // ----------------------------------------------------

        "llen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "llen requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_list = GetRedisListRef(&value_guard)?;

            let len = (redis_list.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        // ----------------------------------------------------
        // lindex key index
        // ----------------------------------------------------

        "lindex" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "lindex requires a key and index".to_string(),
                ));
            }

            let key = command.remove(1);
            let index_bytes = command.remove(1);

            let index: usize = String::from_utf8(index_bytes)
                .map_err(|_| {
                    ServerError::InvalidRedisCommand(
                        "lindex index contains invalid UTF-8".to_string(),
                    )
                })?
                .parse()
                .map_err(|_| {
                    ServerError::InvalidRedisCommand(
                        "lindex index must be a valid number".to_string(),
                    )
                })?;

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_list = GetRedisListRef(&value_guard)?;

            let element = redis_list
                .get(index)
                .ok_or_else(|| {
                    ServerError::InvalidRedisCommand(
                        "list index out of bounds".to_string(),
                    )
                })?;

            Ok(Some(element.clone()))
        }

        // ----------------------------------------------------
        // lset key index value
        // ----------------------------------------------------

        "lset" => {
            if command.len() < 4 {
                return Err(ServerError::InvalidRedisCommand(
                    "lset requires a key, index and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let index_bytes = command.remove(1);
            let new_value = command.remove(1);

            let index: usize = String::from_utf8(index_bytes)
                .map_err(|_| {
                    ServerError::InvalidRedisCommand(
                        "lset index contains invalid UTF-8".to_string(),
                    )
                })?
                .parse()
                .map_err(|_| {
                    ServerError::InvalidRedisCommand(
                        "lset index must be a valid number".to_string(),
                    )
                })?;

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            if index >= redis_list.len() {
                return Err(ServerError::InvalidRedisCommand(
                    "list index out of bounds".to_string(),
                ));
            }

            redis_list.set(index, new_value);

            Ok(None)
        }

        // ----------------------------------------------------
        // lclear key
        // ----------------------------------------------------

        "lclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "lclear requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_list = GetRedisList(&mut value_guard)?;

            redis_list.clear();

            Ok(None)
        }

        _ => {
            Err(ServerError::InvalidRedisCommand(
                format!("command {} not recognized", command_name),
            ))
        }
    }
}


// ============================================================
// HASH
// ============================================================

pub async fn HandleHashOp(
    redis: Arc<RedisServer>,
    command: String,
    _t: String,
) -> Result<Option<Vec<u8>>, ServerError> {

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

        // ----------------------------------------------------
        // hcreate key
        // ----------------------------------------------------

        "hcreate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "hcreate requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            redis.create_hash(key).await;

            Ok(None)
        }

        // ----------------------------------------------------
        // hset key field value
        // ----------------------------------------------------

        "hset" => {
            if command.len() < 4 {
                return Err(ServerError::InvalidRedisCommand(
                    "hset requires a key, field and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_hash = GetRedisHash(&mut value_guard)?;

            redis_hash.set(field, value);

            Ok(None)
        }

        // ----------------------------------------------------
        // hget key field
        // ----------------------------------------------------

        "hget" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "hget requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_hash = GetRedisHashRef(&value_guard)?;

            let value = redis_hash
                .get(&field)
                .ok_or_else(|| {
                    ServerError::InvalidRedisCommand(
                        "hash field does not exist".to_string(),
                    )
                })?;

            Ok(Some(value.to_vec()))
        }

        // ----------------------------------------------------
        // hexists key field
        // ----------------------------------------------------

        "hexists" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "hexists requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_hash = GetRedisHashRef(&value_guard)?;

            let exists = redis_hash.exists(&field);

            Ok(Some(vec![exists as u8]))
        }

        // ----------------------------------------------------
        // hdel key field
        // ----------------------------------------------------

        "hdel" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "hdel requires a key and field".to_string(),
                ));
            }

            let key = command.remove(1);
            let field = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_hash = GetRedisHash(&mut value_guard)?;

            redis_hash.remove(&field);

            Ok(None)
        }

        // ----------------------------------------------------
        // hlen key
        // ----------------------------------------------------

        "hlen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "hlen requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_hash = GetRedisHashRef(&value_guard)?;

            let len = (redis_hash.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        // ----------------------------------------------------
        // hclear key
        // ----------------------------------------------------

        "hclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "hclear requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_hash = GetRedisHash(&mut value_guard)?;

            redis_hash.clear();

            Ok(None)
        }

        // ----------------------------------------------------
        // hkeys key
        // ----------------------------------------------------

        "hkeys" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "hkeys requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_hash = GetRedisHashRef(&value_guard)?;

            let keys = redis_hash.keys();

            let mut result = Vec::new();

            for key in keys {
                result.extend_from_slice(
                    &(key.len() as u64).to_be_bytes()
                );

                result.extend_from_slice(&key);
            }

            Ok(Some(result))
        }

        // ----------------------------------------------------
        // hvalues key
        // ----------------------------------------------------

        "hvalues" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "hvalues requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_hash = GetRedisHashRef(&value_guard)?;

            let values = redis_hash.values();

            let mut result = Vec::new();

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
                format!("command {} not recognized", command_name),
            ))
        }
    }
}


// ============================================================
// SET
// ============================================================

pub async fn HandleSetOp(
    redis: Arc<RedisServer>,
    command: String,
    _t: String,
) -> Result<Option<Vec<u8>>, ServerError> {

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

        // ----------------------------------------------------
        // screate key
        // ----------------------------------------------------

        "screate" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "screate requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            redis.create_set(key).await;

            Ok(None)
        }

        // ----------------------------------------------------
        // sadd key value
        // ----------------------------------------------------

        "sadd" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "sadd requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_set = GetRedisSet(&mut value_guard)?;

            redis_set.add(value);

            Ok(None)
        }

        // ----------------------------------------------------
        // srem key value
        // ----------------------------------------------------

        "srem" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "srem requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_set = GetRedisSet(&mut value_guard)?;

            redis_set.remove(&value);

            Ok(None)
        }

        // ----------------------------------------------------
        // scontains key value
        // ----------------------------------------------------

        "scontains" => {
            if command.len() < 3 {
                return Err(ServerError::InvalidRedisCommand(
                    "scontains requires a key and value".to_string(),
                ));
            }

            let key = command.remove(1);
            let value = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_set = GetRedisSetRef(&value_guard)?;

            let contains = redis_set.contains(&value);

            Ok(Some(vec![contains as u8]))
        }

        // ----------------------------------------------------
        // slen key
        // ----------------------------------------------------

        "slen" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "slen requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_set = GetRedisSetRef(&value_guard)?;

            let len = (redis_set.len() as u64)
                .to_be_bytes()
                .to_vec();

            Ok(Some(len))
        }

        // ----------------------------------------------------
        // sclear key
        // ----------------------------------------------------

        "sclear" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "sclear requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let mut value_guard = value_lock.write().await;

            let redis_set = GetRedisSet(&mut value_guard)?;

            redis_set.clear();

            Ok(None)
        }

        // ----------------------------------------------------
        // svalues key
        // ----------------------------------------------------

        "svalues" => {
            if command.len() < 2 {
                return Err(ServerError::InvalidRedisCommand(
                    "svalues requires a key".to_string(),
                ));
            }

            let key = command.remove(1);

            let value_lock = get_value(&redis, &key).await?;

            let value_guard = value_lock.read().await;

            let redis_set = GetRedisSetRef(&value_guard)?;

            let values = redis_set.values();

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
                format!("command {} not recognized", command_name),
            ))
        }
    }
}


// ============================================================
// STRING HELPERS
// ============================================================

fn GetRedisString(
    value: &mut RedisValue,
) -> Result<&mut RedisString, ServerError> {

    match value {
        RedisValue::String(string) => Ok(string),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a string".to_string(),
        )),
    }
}


fn GetRedisStringRef(
    value: &RedisValue,
) -> Result<&RedisString, ServerError> {

    match value {
        RedisValue::String(string) => Ok(string),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a string".to_string(),
        )),
    }
}


// ============================================================
// LIST HELPERS
// ============================================================

fn GetRedisList(
    value: &mut RedisValue,
) -> Result<&mut RedisList, ServerError> {

    match value {
        RedisValue::List(list) => Ok(list),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a list".to_string(),
        )),
    }
}


fn GetRedisListRef(
    value: &RedisValue,
) -> Result<&RedisList, ServerError> {

    match value {
        RedisValue::List(list) => Ok(list),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a list".to_string(),
        )),
    }
}


// ============================================================
// HASH HELPERS
// ============================================================

fn GetRedisHash(
    value: &mut RedisValue,
) -> Result<&mut RedisHash, ServerError> {

    match value {
        RedisValue::Hash(hash) => Ok(hash),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a hash".to_string(),
        )),
    }
}


fn GetRedisHashRef(
    value: &RedisValue,
) -> Result<&RedisHash, ServerError> {

    match value {
        RedisValue::Hash(hash) => Ok(hash),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a hash".to_string(),
        )),
    }
}


// ============================================================
// SET HELPERS
// ============================================================

fn GetRedisSet(
    value: &mut RedisValue,
) -> Result<&mut RedisSet, ServerError> {

    match value {
        RedisValue::Set(set) => Ok(set),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a set".to_string(),
        )),
    }
}


fn GetRedisSetRef(
    value: &RedisValue,
) -> Result<&RedisSet, ServerError> {

    match value {
        RedisValue::Set(set) => Ok(set),

        _ => Err(ServerError::InvalidRedisType(
            "Redis key does not contain a set".to_string(),
        )),
    }
}
