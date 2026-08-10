use core::hash;
use std::{collections::{HashMap, HashSet}, fs::{File, OpenOptions}, io::{Read, Write}};

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

    //reconstruction
    pub fn replace(&mut self, data: Vec<Vec<u8>>) {
        self.data = data;
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

    //Reconstruction
    pub fn replace(&mut self, data: HashMap<Vec<u8>, Vec<u8>>) {
        self.data = data;
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

    //reconstruction
    pub fn replace(&mut self, data: HashSet<Vec<u8>>) {
        self.data = data;
    }
}


impl Persistence for RedisServer  {
    fn SaveSnapShot(&self) {
        for (key,redis_value)in &self.data{
            let key_len=key.len().to_be_bytes().to_vec();
            
            let value_len;
            match redis_value {
                RedisValue::String(s)=>{
                    let value: &[u8];
                    (value,value_len)=SetStringSnapshot(s);
                    let t="string".as_bytes();
                    SaveStringSnapShot(key, &key_len, &t.to_vec(), &value.to_vec(), &value_len.to_vec());
                }
                RedisValue::List(l)=>{
                    let value: &Vec<Vec<u8>>;
                    let t="list".as_bytes();
                    (value,value_len)=SetListSnapshot(l);
                    SaveListSnapShot(key, &key_len, &t.to_vec(), value, &value_len.to_vec());
                }
                RedisValue::Hash(h)=>{
                    let value:&HashMap<Vec<u8>, Vec<u8>>;
                    let t="hash".as_bytes();
                    (value,value_len)=SetHashSnapshot(h);
                    SaveHashSnapshot(key, &key_len, &t.to_vec(), value, &value_len.to_vec());
                }
                RedisValue::Set(s)=>{
                    let value: &HashSet<Vec<u8>>;
                    let t="set".as_bytes();
                    (value,value_len)=SetSetSnapShot(s);
                    SaveSetSnapShot(key, &key_len, &t.to_vec(), value, &value_len.to_vec());
                }
                _=>{

                }
            }

        }
    }

    fn ReadSnapShot(&self) {
        let mut file = OpenOptions::new()
            .read(true)
            .open("redis.snapshot")?;

        loop{
            let mut key_len_buf=[0u8;8];
            match file.read_exact(&mut key_len_buf){
                Ok((()))=>{
                    let key=ReadBytesFromSnapshot(key_len_buf, &mut file);
                    let mut type_len_buf=[0u8;8];
                    file.read_exact(&mut type_len_buf).unwrap();
                    let t=String::from_utf8(ReadBytesFromSnapshot(type_len_buf, &mut file)).unwrap();
                    

                    match t.trim() {
                        "string"=>{
                            let mut value_len_buf=[0u8;8];
                            file.read_exact(&mut value_len_buf).unwrap();
                            let value=RedisValue::String(RedisString::new(ReadBytesFromSnapshot(value_len_buf, &mut file)));
                            self.data.insert(key, value);
                        }
                        "list"=>{
                            let mut value_len_buf=[0u8;8];
                            file.read_exact(&mut value_len_buf).unwrap();
                            
                            let value=ReadBytesFromSnapshot(value_len_buf, &mut file);
                            let mut redis_list=RedisList::new();
                            redis_list.replace(ReconstructListBytesFromBytes(value));
                            let list=RedisValue::List(redis_list);
                            self.data.insert(key, list);
                        }
                        "hash"=>{
                            let mut value_len_buf=[0u8;8];
                            file.read_exact(&mut value_len_buf).unwrap();
                            let value=ReadBytesFromSnapshot(value_len_buf, &mut file);
                            let hash=RedisHash::new();
                            hash.replace(ReconstructHashBytesFromBytes(value));
                            let hash=RedisValue::Hash(hash);
                            self.data.insert(key, hash);
                        }
                        "set"=>{
                            let mut value_len_buf=[0u8;8];
                            file.read_exact(&mut value_len_buf).unwrap();
                            let value=ReadBytesFromSnapshot(value_len_buf, &mut file);
                            let set=RedisSet::new();
                            set.replace(ReconstructSetBytesFromBytes(value));
                            let set=RedisValue::Set(set);
                            self.data.insert(key, set);
                        }
                        _=>{

                        }
                    }
                    

                }
                
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Reached EOF
                    break;
                }

                Err(e) => {
                    // Some real error occurred
                    
                }
            }
        }
    }
}

fn SetStringSnapshot(redis_string:&RedisString)->(&[u8],[u8;8]){
    let value=redis_string.get();
    let value_len=value.len().to_be_bytes();
    (value,value_len)
}

fn SetListSnapshot(redis_list:&RedisList)->(&Vec<Vec<u8>>,[u8;8]){
    let value=&redis_list.data;
    let mut value_len = 0;

    for element in value {
        // element length field
        value_len += 8;

        // actual element bytes
        value_len += element.len();
    }
    (value,value_len.to_be_bytes())
}

fn SetHashSnapshot(redis_hash:&RedisHash)->(&HashMap<Vec<u8>, Vec<u8>>,[u8;8]){
    let value=&redis_hash.data;
    let mut value_len = 0;
    
    for (k, v) in value {
        // key length,see in teh savehashsnapshot ther we stre teh first len adn then value/key
        value_len += 8;

        // key
        value_len += k.len();

        // value length
        value_len += 8;

        // value
        value_len += v.len();
    }

    (value, value_len.to_be_bytes())
}

fn SetSetSnapShot(redis_set:&RedisSet)->(&HashSet<Vec<u8>>,[u8;8]){
    let value=&redis_set.data;
    let mut value_len = 0;

    for element in value {
        // element length field
        value_len += 8;

        // actual element bytes
        value_len += element.len();
    }
    (value,value_len.to_be_bytes())
}

fn SaveStringSnapShot(key:&Vec<u8>,key_len:&Vec<u8>,t:&Vec<u8>,value:&Vec<u8>,value_len:&Vec<u8>){
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("redis.snapshot")?;
    let mut buf = Vec::new();

    buf.extend_from_slice(key_len);
    buf.extend_from_slice(key);
    let type_len = t.len().to_be_bytes();
    buf.extend_from_slice(&type_len);
    buf.extend_from_slice(t);
    buf.extend_from_slice(value_len);
    buf.extend_from_slice(value);

    file.write_all(&buf);

}

fn SaveListSnapShot(key:&Vec<u8>,key_len:&Vec<u8>,t:&Vec<u8>,value:&Vec<Vec<u8>>,value_len:&Vec<u8>){
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("redis.snapshot")?;
    let mut buf = Vec::new();

    buf.extend_from_slice(key_len);
    buf.extend_from_slice(key);
    let type_len = t.len().to_be_bytes();
    buf.extend_from_slice(&type_len);
    buf.extend_from_slice(t);
    buf.extend_from_slice(value_len);


    // each list element
    for element in value {
        let element_len = element.len().to_be_bytes();

        buf.extend_from_slice(&element_len);
        buf.extend_from_slice(element);
    }

    file.write_all(&buf)?;
}

fn SaveHashSnapshot(key:&Vec<u8>,key_len:&Vec<u8>,t:&Vec<u8>,value:&HashMap<Vec<u8>, Vec<u8>>,value_len:&Vec<u8>){
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("redis.snapshot")?;
    let mut buf = Vec::new();

    buf.extend_from_slice(key_len);
    buf.extend_from_slice(key);
    let type_len = t.len().to_be_bytes();
    buf.extend_from_slice(&type_len);
    buf.extend_from_slice(t);
    buf.extend_from_slice(value_len);
   
    for (k,v) in value{
        let key_len=k.len().to_be_bytes();
        buf.extend_from_slice(&key_len);
        buf.extend_from_slice(k);

        let v_len=v.len().to_be_bytes();
        buf.extend_from_slice(&v_len);
        buf.extend_from_slice(v);

    }

    file.write_all(&buf)?;
}

fn SaveSetSnapShot(key:&Vec<u8>,key_len:&Vec<u8>,t:&Vec<u8>,value:&HashSet<Vec<u8>>,value_len:&Vec<u8>){
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("redis.snapshot")?;
    let mut buf = Vec::new();

    buf.extend_from_slice(key_len);
    buf.extend_from_slice(key);
    let type_len = t.len().to_be_bytes();
    buf.extend_from_slice(&type_len);
    buf.extend_from_slice(t);
    buf.extend_from_slice(value_len);
   
    

    // each set element
    for element in value {
        let element_len = element.len().to_be_bytes();

        buf.extend_from_slice(&element_len);
        buf.extend_from_slice(element);
    }

    file.write_all(&buf)?;
}

fn TruncateOldSnapshot() -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("redis.snapshot")?;

    Ok(())
}

fn ReadBytesFromSnapshot(len_buf:[u8;8],file:&mut File)->Vec<u8>{
    let len = usize::from_be_bytes(len_buf);

    let mut buf = vec![0u8; len];

    file.read_exact(&mut buf).unwrap();

    buf
    
}

fn ReconstructListBytesFromBytes(value: Vec<u8>) -> Vec<Vec<u8>> {
    let mut list: Vec<Vec<u8>> = Vec::new();
    let mut position = 0;

    while position < value.len() {
        // Read element length
        let mut len_buf = [0u8; 8];

        len_buf.copy_from_slice(&value[position..position + 8]);
        position += 8;

        let element_len = usize::from_be_bytes(len_buf);

        // Read element
        let element = value[position..position + element_len].to_vec();
        position += element_len;

        list.push(element);
    }

    list
}

fn ReconstructHashBytesFromBytes(value: Vec<u8>) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut position = 0;

    while position < value.len() {

        // Read hash key length
        let mut key_len_buf = [0u8; 8];

        key_len_buf.copy_from_slice(
            &value[position..position + 8]
        );

        position += 8;

        let key_len = usize::from_be_bytes(key_len_buf);

        // Read hash key
        let key = value[position..position + key_len].to_vec();

        position += key_len;

        // Read hash value length
        let mut value_len_buf = [0u8; 8];

        value_len_buf.copy_from_slice(
            &value[position..position + 8]
        );

        position += 8;

        let value_len = usize::from_be_bytes(value_len_buf);

        // Read hash value
        let hash_value =
            value[position..position + value_len].to_vec();

        position += value_len;

        map.insert(key, hash_value);
    }

    map

}

fn ReconstructSetBytesFromBytes(value: Vec<u8>) -> HashSet<Vec<u8>> {
    let mut set: HashSet<Vec<u8>> = HashSet::new();
    let mut position = 0;

    while position < value.len() {
        // Read element length
        let mut len_buf = [0u8; 8];

        len_buf.copy_from_slice(&value[position..position + 8]);
        position += 8;

        let element_len = usize::from_be_bytes(len_buf);

        // Read element
        let element = value[position..position + element_len].to_vec();
        position += element_len;

        set.insert(element);
    }

    set
}
