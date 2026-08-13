use std::{collections::HashMap, fs::OpenOptions, io::Write, sync::Arc, time::Instant};

use tokio::{io::AsyncWriteExt, sync::mpsc};

use crate::{error::ServerError, server::redis::{RedisHash, RedisList, RedisServer, RedisSet, RedisString, RedisValue}};






pub fn WriteToLog(command:String) {
    let content = format!("{} \n", command);

    let mut file = create_if_not_exists("redis.log").unwrap();

    file.write_all(content.as_bytes()).unwrap();
   
}

pub fn ReadLog()->String {
    let mut file=create_if_not_exists("redis.log").unwrap();
    let data=std::fs::read("redis.log").unwrap();
    let content=String::from_utf8(data).unwrap();
    content
}

pub fn ReconstructLogFile(content:String)->HashMap<Vec<u8>,RedisValue>{
    let mut map=HashMap::new();

    for operation in content.lines(){
        let commands:Vec<&str>=operation.split_whitespace().collect();
        match commands[0] {
            "set"=>{
                let key=commands[1].as_bytes().to_vec();
                let value=RedisValue::String(RedisString::new(commands[2].as_bytes().to_vec()) ) ;

                map.insert(key, value);
            }
            "get"=>{}
            "append"=>{
                let key=commands[1].as_bytes().to_vec();
                
                let value=RedisValue::String(RedisString::new(commands[2].as_bytes().to_vec()) ) ;

                map.insert(key, value);
            }
            "len"=>{

            }


            "lcreate"=>{
                let key=commands[1].as_bytes().to_vec();
                let empty_list=RedisValue::List(RedisList::new());

                map.insert(key, empty_list);
            }
            "lpush"=>{
                let key=commands[1].as_bytes().to_vec();
                let value=commands[2].as_bytes().to_vec();

                let redis_value=map.get_mut(&key).unwrap();

                match redis_value {
                    RedisValue::List(redis_list)=>{
                        redis_list.push_front(value);
                    }
                    _=>{}
                }
            }
            "rpush"=>{
                let key=commands[1].as_bytes().to_vec();
                let value=commands[2].as_bytes().to_vec();

                let redis_value=map.get_mut(&key).unwrap();

                match redis_value {
                    RedisValue::List(redis_list)=>{
                        redis_list.push_back(value);
                    }
                    _=>{}
                }
            }
            "lpop" => {
                let key = commands[1].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::List(redis_list) = redis_value {
                    redis_list.pop_front();
                }
            }

            "rpop" => {
                let key = commands[1].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::List(redis_list) = redis_value {
                    redis_list.pop_back();
                }
            }

            "lset" => {
                let key = commands[1].as_bytes().to_vec();
                let index: usize = commands[2].parse().unwrap();
                let value = commands[3].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::List(redis_list) = redis_value {
                    redis_list.set(index, value);
                }
            }

            "lclear" => {
                let key = commands[1].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::List(redis_list) = redis_value {
                    redis_list.clear();
                }
            }

            "hcreate" => {
                let key = commands[1].as_bytes().to_vec();

                map.insert(
                    key,
                    RedisValue::Hash(RedisHash::new())
                );
            }

            "hset" => {
                let key = commands[1].as_bytes().to_vec();
                let field = commands[2].as_bytes().to_vec();
                let value = commands[3].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Hash(redis_hash) = redis_value {
                    redis_hash.set(field, value);
                }
            }

            "hdel" => {
                let key = commands[1].as_bytes().to_vec();
                let field = commands[2].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Hash(redis_hash) = redis_value {
                    redis_hash.remove(&field);
                }
            }

            "hclear" => {
                let key = commands[1].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Hash(redis_hash) = redis_value {
                    redis_hash.clear();
                }
            }

            "screate" => {
                let key = commands[1].as_bytes().to_vec();

                map.insert(
                    key,
                    RedisValue::Set(RedisSet::new())
                );
            }

            "sadd" => {
                let key = commands[1].as_bytes().to_vec();
                let value = commands[2].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Set(redis_set) = redis_value {
                    redis_set.add(value);
                }
            }

            "srem" => {
                let key = commands[1].as_bytes().to_vec();
                let value = commands[2].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Set(redis_set) = redis_value {
                    redis_set.remove(&value);
                }
            }

            "sclear" => {
                let key = commands[1].as_bytes().to_vec();

                let redis_value = map.get_mut(&key).unwrap();

                if let RedisValue::Set(redis_set) = redis_value {
                    redis_set.clear();
                }
            }
            _=>{
            
            }
        }
    }

    map
}

fn Comapaction(){

}

    // fn ReadSnapShot(&mut self) -> Result<(), ServerError>;

    // fn SaveSnapShot(& self)-> Result<(), ServerError>;
    


fn create_if_not_exists(path: &str) -> Result<std::fs::File,Box<dyn std::error::Error>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    Ok(file)
}


// pub fn start_logger() -> mpsc::Sender<String> {
//     let (tx, mut rx) = mpsc::channel::<String>(1_000);

//     tokio::spawn(async move {
//         let mut file = match tokio::fs::OpenOptions::new()
//             .create(true)
//             .append(true)
//             .open("redis.log")
//             .await
//         {
//             Ok(file) => file,

//             Err(e) => {
//                 eprintln!("Failed to open redis.log: {}", e);
//                 return;
//             }
//         };

//         let mut batch = Vec::with_capacity(1000);

//         while let Some(command) = rx.recv().await {
//             batch.push(command);

           
//             while batch.len() < 1000 {
//                 match rx.try_recv() {
//                     Ok(command) => {
//                         batch.push(command);
//                     }

//                     Err(_) => {
//                         break;
//                     }
//                 }
//             }

//             // Build one buffer
//             let mut buffer = Vec::new();

//             for command in batch.drain(..) {
//                 buffer.extend_from_slice(command.as_bytes());
//                 buffer.push(b'\n');
//             }

//             // One filesystem write for the entire batch
//             if let Err(e) = file.write_all(&buffer).await {
//                 eprintln!("Failed to write log: {}", e);
//             }
//         }

//         if let Err(e) = file.flush().await {
//             eprintln!("Failed to flush log: {}", e);
//         }
//     });

//     tx
// }




// pub fn start_logger() -> mpsc::Sender<String> {
//     let (tx, mut rx) = mpsc::channel::<String>(1_000);

//     tokio::spawn(async move {
//         let mut file = match tokio::fs::OpenOptions::new()
//             .create(true)
//             .append(true)
//             .open("redis.log")
//             .await
//         {
//             Ok(file) => file,

//             Err(e) => {
//                 eprintln!("Failed to open redis.log: {}", e);
//                 return;
//             }
//         };

//         let mut batch = Vec::with_capacity(1000);
//         let mut buffer = Vec::with_capacity(64 * 1024);

//         let mut total_write_time = std::time::Duration::ZERO;
//         let mut total_bytes = 0usize;
//         let mut total_writes = 0usize;

//         while let Some(command) = rx.recv().await {
//             batch.push(command);

//             while batch.len() < 1000 {
//                 match rx.try_recv() {
//                     Ok(command) => {
//                         batch.push(command);
//                     }

//                     Err(_) => {
//                         break;
//                     }
//                 }
//             }

//             buffer.clear();

//             for command in batch.drain(..) {
//                 buffer.extend_from_slice(command.as_bytes());
//                 buffer.push(b'\n');
//             }

//             let start = Instant::now();

//             if let Err(e) = file.write_all(&buffer).await {
//                 eprintln!("Failed to write log: {}", e);
//                 continue;
//             }

//             let elapsed = start.elapsed();

//             total_write_time += elapsed;
//             total_bytes += buffer.len();
//             total_writes += 1;
//             println!(
//             "Logger finished: {} writes, {} bytes, total write time: {:?}",
//             total_writes,
//             total_bytes,
//             total_write_time
//         );
//         }

//         if let Err(e) = file.flush().await {
//             eprintln!("Failed to flush log: {}", e);
//         }

        
//     });

//     tx
// }


pub fn start_logger() -> mpsc::Sender<String> {
    let (tx, mut rx) = mpsc::channel::<String>(10_000);

    tokio::spawn(async move {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("redis.log")
            .await
        {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to open redis.log: {}", e);
                return;
            }
        };

        let mut buffer = Vec::with_capacity(64 * 1024);

        loop {
            // Wait for the first command.
            let first = match rx.recv().await {
                Some(command) => command,
                None => break,
            };

            buffer.clear();

            buffer.extend_from_slice(first.as_bytes());
            buffer.push(b'\n');

            // Give the channel a short window to accumulate more commands.
            let deadline =
                tokio::time::sleep(std::time::Duration::from_millis(1));

            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    command = rx.recv() => {
                        match command {
                            Some(command) => {
                                buffer.extend_from_slice(command.as_bytes());
                                buffer.push(b'\n');

                                // Don't let the batch become too large.
                                if buffer.len() >= 64 * 1024 {
                                    break;
                                }
                            }

                            None => {
                                break;
                            }
                        }
                    }

                    _ = &mut deadline => {
                        break;
                    }
                }
            }

            // let start = Instant::now();

            if let Err(e) = file.write_all(&buffer).await {
                eprintln!("Failed to write log: {}", e);
                continue;
            }

            // let elapsed = start.elapsed();

            // println!(
            //     "Logger: wrote {} bytes in {:?}",
            //     buffer.len(),
            //     elapsed
            // );
        }

        if let Err(e) = file.flush().await {
            eprintln!("Failed to flush log: {}", e);
        }
    });

    tx
}





