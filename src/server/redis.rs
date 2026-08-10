use std::collections::{HashMap, HashSet};

use crate::{error::ServerError, server::persistenc::Persistence};






// pub enum RedisValues{
//     Strings(Vec<u8>),
//     List(Vec<Vec<u8>>),
//     HashMap(HashMap<Vec<u8>,Vec<u8>>),
//     Set(HashSet<Vec<u8>>)
// }

// pub struct redis_server{
//     data:HashMap<Vec<u8>,RedisValues>,   
// }





// impl redis_server {
//     pub fn new()->Self{
//         let content =self::redis_server::ReadLog();
//         let map=self::redis_server::ReconstructLogFile(content);
//         Self { 
//             data: map
//         }
//     }

//     fn set(&mut self,key:Vec<u8>,value:Vec<u8>){//value is u8 as set is simple key value storage 

//         let prev_value= self.data
//         .insert(key.clone(), RedisValues::Strings(value.clone()));

//         match prev_value{
//             Some(val)=>{
//                 match val {
//                     RedisValues::Strings(val)=>{
//                         let key=String::from_utf8(key).unwrap();
//                         let value=String::from_utf8(value).unwrap();
//                         let val=String::from_utf8(val).unwrap();

//                         let command=String::from("set");
//                         let args=[key.clone(),value.clone()];
                        
//                         self.WriteToLog(command, &args);

//                         println!("Server: Prev value of hte key {} is {} and has been replace with {}",key,val,value);
//                     }
//                     _=>{

//                     }
//                 }

                
//             }
//             None=>{
//                 println!("Server: Insertion Successful");
//             }
//         }
        
    
//     }

    

//     fn get(&self,key:Vec<u8>)->Result<String, ServerError>{
//         let value=self.data.get(&key);

//         match value {
//             Some(val)=>{
//                 match val {
//                     RedisValues::Strings(val)=>{
//                         let val=String::from_utf8(val.to_owned()).unwrap();
//                         Ok(val)
//                     }
//                     _=>{
//                         Err(())
//                     }
//                 }
                
//             }
//             None=>{
//                 Err(ServerError::NoRedisKey(String::from("No mathcing key found")))
//             }
//         }
//     }
   


//     fn list_create(&mut self,key:Vec<u8>){
//         let empty_list=RedisValues::List([[0u8].to_vec()].to_vec());
//         self.data.insert(key, empty_list);
//     }

//     fn list_add_start(&mut self,key:Vec<u8>,value:Vec<u8>){
//         let list =self.data.get_mut(&key);
//         match list {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::List(list)=>{
//                         list.insert(0, value);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_add_end(&mut self,key:Vec<u8>,value:Vec<u8>){
//         let list =self.data.get_mut(&key);
//         match list {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::List(list)=>{
//                         list.push(value);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     } 

//     fn list_remove_start(&mut self,key:Vec<u8>){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         list.remove(0);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_remove_end(&mut self,key:Vec<u8>){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         list.pop();
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_len(&mut self,key:Vec<u8>){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         let len=list.len();
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_ele(&mut self,key:Vec<u8>,index:&[u8]){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         let index: u32 = u32::from_be_bytes(index.try_into().unwrap());
//                         let buf = &list[index as usize];
//                         let element=String::from_utf8(buf.to_owned()).unwrap();
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_set(&mut self,key:Vec<u8>,index:&[u8],element:&[u8]){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         let index: u32 = u32::from_be_bytes(index.try_into().unwrap());
//                         list[index as usize]=element.to_vec();
                        
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn list_range(&mut self,key:Vec<u8>,start:&[u8],end:&[u8]){
//         let value=self.data.get_mut(&key);

//         match value {
//             Some(rediis_values)=>{
//                 match rediis_values {
//                     RedisValues::List(list)=>{
//                         let start_index= u32::from_be_bytes(start.try_into().unwrap()) as usize;
//                         let end_index = u32::from_be_bytes(end.try_into().unwrap())as usize;
                        
//                         let elements = &list[start_index..=end_index];
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn hash_create(&mut self,outer_key:Vec<u8>){
//         self.data.insert(outer_key, RedisValues::HashMap(HashMap::new()));
        
//     }

//     fn hash_insert(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>,inner_value:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         inner_map.insert(inner_key, inner_value);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn hash_get(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let inner_value=inner_map.get(&inner_key);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn hash_delete(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let ifvalue=inner_map.remove(&inner_key);

//                         match ifvalue {
//                             Some(value)=>{

//                             }
//                             None=>{

//                             }
//                         }
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn hash_exist(&mut self,outer_key:Vec<u8>,inner_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let ifvalue=inner_map.contains_key(&inner_key);

//                         match ifvalue {
//                             true=>{

//                             }
//                             _=>{

//                             }
//                         }
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }
 
//     fn hash_len(&mut self,outer_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let size=inner_map.len();

                        
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }
 
//     fn hash_get_all(&mut self,outer_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
                        
                        
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }
    
//     fn hash_get_all_keys(&mut self,outer_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let keys=inner_map.keys();
//                         for key in keys{

//                         }
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }
    
//     fn hash_get_all_values(&mut self,outer_key:Vec<u8>){
//         let outer_map=self.data.get_mut(&outer_key);
//         match outer_map {
//             Some(redisvalue)=>{
//                 match redisvalue {
//                     RedisValues::HashMap(inner_map)=>{
//                         let values=inner_map.values();
//                         for value in values{

//                         }
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }
//         }
//     }

//     fn set_add(&mut self,key:Vec<u8>,value:Vec<u8>){
//         let is_redis_values=self.data.get_mut(&key);
//         match is_redis_values {
//             Some(redis_values)=>{
//                 match redis_values {
//                     RedisValues::Set(set)=>{
//                         let isinsert= set.insert(value);
//                     }
//                     _=>{

//                     }
//                 }
//             }
//             None=>{

//             }

//         }
//     }

// }
pub enum RedisValue {
    String(RedisString),
    List(RedisList),
    Hash(RedisHash),
    Set(RedisSet),
}

pub struct RedisServer {pub data: HashMap<Vec<u8>, RedisValue>,}

pub struct RedisString {data: Vec<u8>,}

pub struct RedisList {data: Vec<Vec<u8>>}

pub struct RedisHash {data: HashMap<Vec<u8>, Vec<u8>>}

pub struct RedisSet {data: HashSet<Vec<u8>>}

//
// ---------------- RedisServer ----------------
//

impl RedisServer {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn create_string(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data
            .insert(key, RedisValue::String(RedisString::new(value)));
    }

    pub fn create_list(&mut self, key: Vec<u8>) {
        self.data
            .insert(key, RedisValue::List(RedisList::new()));
    }

    pub fn create_hash(&mut self, key: Vec<u8>) {
        self.data
            .insert(key, RedisValue::Hash(RedisHash::new()));
    }

    pub fn create_set(&mut self, key: Vec<u8>) {
        self.data
            .insert(key, RedisValue::Set(RedisSet::new()));
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.data.remove(key);
    }
}

//
// ---------------- RedisString ----------------
//

impl RedisString {
    pub fn new(value: Vec<u8>) -> Self {
        Self { data: value }
    }

    pub fn get(&self) -> &[u8] {
        &self.data
    }

    pub fn append(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

//
// ---------------- RedisList ----------------
//

impl RedisList {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
        }
    }

    pub fn push_front(&mut self, value: Vec<u8>) {
        self.data.insert(0, value);
    }

    pub fn push_back(&mut self, value: Vec<u8>) {
        self.data.push(value);
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        if self.data.is_empty() {
            None
        } else {
            Some(self.data.remove(0))
        }
    }

    pub fn pop_back(&mut self) -> Option<Vec<u8>> {
        self.data.pop()
    }

    pub fn get(&self, index: usize) -> Option<&Vec<u8>> {
        self.data.get(index)
    }

    pub fn set(&mut self, index: usize, value: Vec<u8>) {
        if index < self.data.len() {
            self.data[index] = value;
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}

//
// ---------------- RedisHash ----------------
//

impl RedisHash {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, field: Vec<u8>, value: Vec<u8>) {
        self.data.insert(field, value);
    }

    pub fn get(&self, field: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(field)
    }

    pub fn exists(&self, field: &[u8]) -> bool {
        self.data.contains_key(field)
    }

    pub fn remove(&mut self, field: &[u8]) {
        self.data.remove(field);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data.keys().cloned().collect()
    }

    pub fn values(&self) -> Vec<Vec<u8>> {
        self.data.values().cloned().collect()
    }
}

//
// ---------------- RedisSet ----------------
//

impl RedisSet {
    pub fn new() -> Self {
        Self {
            data: HashSet::new(),
        }
    }

    pub fn add(&mut self, value: Vec<u8>) {
        self.data.insert(value);
    }

    pub fn remove(&mut self, value: &[u8]) {
        self.data.remove(value);
    }

    pub fn contains(&self, value: &[u8]) -> bool {
        self.data.contains(value)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn values(&self) -> Vec<Vec<u8>> {
        self.data.iter().cloned().collect()
    }
}


impl Persistence for RedisServer  {
    fn SaveSnapShot(&self) {
        

        for (key,redis_value)in &self.data{
            let key_len=key.len().to_be_bytes();
            
            let value_len;
            match redis_value {
                RedisValue::String(s)=>{
                    let value;
                    (value,value_len)=GetStringSnapshot(s);
                }
                RedisValue::List(l)=>{
                    let value;
                    (value,value_len)=GetListSnapshot(l);
                }
                RedisValue::Hash(h)=>{
                    let value;
                    (value,value_len)=GetHashSnapshot(h);
                }
                RedisValue::Set(s)=>{
                    let value;
                    (value,value_len)=GetSetSnapShot(s);
                }
                _=>{

                }
            }

        }
    }
}

fn GetStringSnapshot(redis_string:&RedisString)->(&[u8],[u8;8]){
    let value=redis_string.get();
    let value_len=value.len().to_be_bytes();
    (value,value_len)
}

fn GetListSnapshot(redis_list:&RedisList)->(&Vec<Vec<u8>>,[u8;8]){
    let value=&redis_list.data;
    let value_len=value.len().to_be_bytes();
    (value,value_len)
}

fn GetHashSnapshot(redis_hash:&RedisHash)->(&HashMap<Vec<u8>, Vec<u8>>,[u8;8]){
    let value=&redis_hash.data;
    let value_len=value.len().to_be_bytes();
    (value,value_len)
}

fn GetSetSnapShot(redis_set:&RedisSet)->(&HashSet<Vec<u8>>,[u8;8]){
    let value=&redis_set.data;
    let value_len=value.len().to_be_bytes();
    (value,value_len)
}