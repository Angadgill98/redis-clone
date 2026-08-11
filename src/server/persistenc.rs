use std::{collections::HashMap, fs::OpenOptions, io::Write};

use crate::{error::ServerError, server::redis::{RedisHash, RedisList, RedisSet, RedisString, RedisValue}};





pub trait Persistence {
    fn WriteToLog(&self,command:String) {
        let content = format!("{} \n", command);

        let mut file = create_if_not_exists("redis.log").unwrap();

        file.write_all(content.as_bytes()).unwrap();
    }
    
    fn ReadLog(&self)->String {
        let mut file=create_if_not_exists("redis.log").unwrap();
        let data=std::fs::read("redis.log").unwrap();
        let content=String::from_utf8(data).unwrap();
        content
    }

    fn ReconstructLogFile(&self,content:String)->HashMap<Vec<u8>,RedisValue>{
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

    fn ReadSnapShot(&mut self) -> Result<(), ServerError>;

    fn SaveSnapShot(& self)-> Result<(), ServerError>;
    
}

fn create_if_not_exists(path: &str) -> Result<std::fs::File,Box<dyn std::error::Error>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    Ok(file)
}

