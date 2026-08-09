
use std::{collections::{HashMap, HashSet, hash_map}, fs::OpenOptions, io::{Read, Write}, net::{TcpListener, TcpStream}, string};

use crate::{error::ServerError, server::persistenc::{self, Persistence}};



pub fn Init(sender:std::sync::mpsc::Sender<u8>)->Result<(),ServerError>{
    let socket = CreateSocket()?;

    let (stream,client_addr)=socket.accept()?;

    sender.send(1).unwrap();
    
    let redis=redis_server::new(stream);


    Ok(())
}

fn CreateSocket()->Result<TcpListener,ServerError>{
    let addr=std::env::var("redis_server_addr")?;

    let socket =std::net::TcpListener::bind(addr)?;

    Ok(socket)
}

enum RedisValues{
    Strings(Vec<u8>),
    List(Vec<Vec<u8>>),
    HashMap(HashMap<Vec<u8>,Vec<u8>>),
    Set(HashSet<Vec<u8>>)
}

struct redis_server{
    socket:TcpStream,
    data:hash_map::HashMap<Vec<u8>,RedisValues>
}

impl redis_server {
    fn new(socket:TcpStream)->Self{
        let content =self::redis_server::ReadLog();
        let map=self::redis_server::ReconstructLogFile(content);
        Self { 
            socket: socket,
            data: map
        }
    }

    fn set(&mut self,key:Vec<u8>,value:RedisValues){//value is u8 as set is simple key value storage 
        match value {
            RedisValues::Strings(value)=>{
                let prev_value= self.data
                .insert(key.clone(), value.clone());

                match prev_value{
                    Some(val)=>{
                        

                        let key=String::from_utf8(key).unwrap();
                        let value=String::from_utf8(value).unwrap();
                        let val=String::from_utf8(val).unwrap();

                        let command=String::from("set");
                        let args=[key.clone(),value.clone()];
                        
                        self.WriteToLog(command, &args);

                        println!("Server: Prev value of hte key {} is {} and has been replace with {}",key,val,value);
                    }
                    None=>{
                        println!("Server: Insertion Successful");
                    }
                }
                let success=1;
                self.socket.write_all(&[success]).unwrap();
                let len=0;
                self.socket.write_all(&[len]).unwrap();

            }
            _=>{
                println!("Server:value of not desired type");
            }
        }
        
    }

    fn get(&self,key:Vec<u8>)->Result<String, ServerError>{
        let value=self.data.get(&key);

        match value {
            Some(val)=>{
                match val {
                    RedisValues::Strings(val)=>{
                        let val=String::from_utf8(val.to_owned()).unwrap();
                        Ok(val)
                    }
                    _=>{
                        Err(())
                    }
                }
                
            }
            None=>{
                Err(ServerError::NoRedisKey(String::from("No mathcing key found")))
            }
        }
    }
   
    fn list_create(&mut self,key:Vec<u8>){
        let empty_list=RedisValues::List([[0u8].to_vec()].to_vec());
        self.data.insert(key, empty_list);
    }

    fn list_add_start(&mut self,key:Vec<u8>,value:Vec<u8>){
        let list =self.data.get_mut(&key);
        match list {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::List(list)=>{
                        list.insert(0, value);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_add_end(&mut self,key:Vec<u8>,value:Vec<u8>){
        let list =self.data.get_mut(&key);
        match list {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::List(list)=>{
                        list.push(value);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    } 

    fn list_remove_start(&mut self,key:Vec<u8>){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        list.remove(0);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_remove_end(&mut self,key:Vec<u8>){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        list.pop();
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_len(&mut self,key:Vec<u8>){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        let len=list.len();
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_ele(&mut self,key:Vec<u8>,index:&[u8]){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        let index: u32 = u32::from_be_bytes(index.try_into().unwrap());
                        let buf = &list[index as usize];
                        let element=String::from_utf8(buf.to_owned()).unwrap();
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_set(&mut self,key:Vec<u8>,index:&[u8],element:&[u8]){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        let index: u32 = u32::from_be_bytes(index.try_into().unwrap());
                        list[index as usize]=element.to_vec();
                        
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn list_range(&mut self,key:Vec<u8>,start:&[u8],end:&[u8]){
        let value=self.data.get_mut(&key);

        match value {
            Some(rediis_values)=>{
                match rediis_values {
                    RedisValues::List(list)=>{
                        let start_index= u32::from_be_bytes(start.try_into().unwrap()) as usize;
                        let end_index = u32::from_be_bytes(end.try_into().unwrap())as usize;
                        
                        let elements = &list[start_index..=end_index];
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn hash_create(&mut self,outer_key:Vec<u8>){
        self.data.insert(outer_key, RedisValues::HashMap(HashMap::new()));
        
    }

    fn hash_insert(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>,inner_value:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        inner_map.insert(inner_key, inner_value);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn hash_get(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let inner_value=inner_map.get(inner_key);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn hash_delete(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let ifvalue=inner_map.remove(&inner_key);

                        match ifvalue {
                            Some(value)=>{

                            }
                            None=>{

                            }
                        }
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn hash_exist(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let ifvalue=inner_map.contains_key(&inner_key);

                        match ifvalue {
                            true=>{

                            }
                            _=>{

                            }
                        }
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }
 
    fn hash_len(&mut self,outer_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let size=inner_map.len();

                        
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }
 
    fn hash_get_all(&mut self,outer_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        
                        
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }
    
    fn hash_get_all_keys(&mut self,outer_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let keys=inner_map.keys();
                        for key in keys{

                        }
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }
    
    fn hash_get_all_values(&mut self,outer_key:Vec<u8>){
        let outer_map=self.data.get_mut(&outer_key);
        match outer_map {
            Some(redisvalue)=>{
                match redisvalue {
                    RedisValues::HashMap(inner_map)=>{
                        let values=inner_map.values();
                        for value in values{

                        }
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }
        }
    }

    fn set_add(&mut self,key:Vec<u8>,value:Vec<u8>){
        let is_redis_values=self.data.get_mut(key);
        match is_redis_values {
            Some(redis_values)=>{
                match redis_values {
                    RedisValues::Set(set)=>{
                        let isinsert= set.insert(value);
                    }
                    _=>{

                    }
                }
            }
            None=>{

            }

        }
    }
}


impl persistenc::Persistence for redis_server  {
    
}

