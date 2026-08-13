use core::hash;
use std::{collections::{HashMap, HashSet}, fs::{File, OpenOptions}, io::{Read, Write}, net::SocketAddr, sync::Arc};





use tokio::{net::tcp::OwnedWriteHalf, sync::{Mutex, RwLock}};

use crate::{error::ServerError};

#[derive(Debug)]
pub enum RedisValue {
    String(RedisString),
    List(RedisList),
    Hash(RedisHash),
    Set(RedisSet),
    // PubSub(RedisPubSub),
}

#[derive(Debug)]
pub struct RedisServer {pub data: RwLock<HashMap<Vec<u8>, Arc<RwLock<RedisValue>>>>,pub Channels:RwLock<HashMap<Vec<u8>,RwLock<HashSet<SocketAddr>>>>,pub Clients: RwLock<HashMap<SocketAddr, Arc<Mutex<OwnedWriteHalf>>>>}



#[derive(Debug)]
pub struct RedisString {data: Vec<u8>,}

#[derive(Debug)]
pub struct RedisList {data: Vec<Vec<u8>>}

#[derive(Debug)]
pub struct RedisHash {data: HashMap<Vec<u8>, Vec<u8>>}

#[derive(Debug)]
pub struct RedisSet {data: HashSet<Vec<u8>>}

// #[derive(Debug)]
// pub struct RedisPubSub {data: Vec<WriteHalf>}

//
// ---------------- RedisServer ----------------
//

impl RedisServer {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            Channels: RwLock::new(HashMap::new()),
            Clients: RwLock::new(HashMap::new())
        }
    }

    pub async fn create_string(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data
            .write()
            .await
            .insert(key, Arc::new(RwLock::new(RedisValue::String(RedisString::new(value)))));
    }

    pub async fn create_list(&self, key: Vec<u8>) {
        self.data
            .write()
            .await
            .insert(key, Arc::new(RwLock::new(RedisValue::List(RedisList::new()))));
    }

    pub async  fn create_hash(&self, key: Vec<u8>) {
        self.data
            .write()
            .await
            .insert(key, Arc::new(RwLock::new(RedisValue::Hash(RedisHash::new()))));
    }

    pub async fn create_set(&self, key: Vec<u8>) {
        self.data
            .write()
            .await
            .insert(key, Arc::new(RwLock::new(RedisValue::Set(RedisSet::new()))));
    }

    pub async fn exists(&self, key: &[u8]) -> bool {
        self.data.read().await.contains_key(key)
    }

    pub async fn delete(&mut self, key: &[u8]) {
        self.data
            .write()
            .await
            .remove(key);
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

// impl RedisServer {
//     async fn SaveSnapShot(&self) -> Result<(), ServerError> {
//         // Replace the old snapshot completely before writing
//         TruncateOldSnapshot()?;
//         let data = self.data.read().await;

//         for (key, redis_value) in data.iter() {
//             let key_len = key.len().to_be_bytes();

//             match redis_value {
//                 RedisValue::String(s) => {
//                     let (value, value_len) = SetStringSnapshot(s);
//                     let t = b"string";

//                     SaveStringSnapShot(
//                         key,
//                         &key_len,
//                         t,
//                         value,
//                         &value_len,
//                     )?;
//                 }

//                 RedisValue::List(l) => {
//                     let (value, value_len) = SetListSnapshot(l);
//                     let t = b"list";

//                     SaveListSnapShot(
//                         key,
//                         &key_len,
//                         t,
//                         value,
//                         &value_len,
//                     )?;
//                 }

//                 RedisValue::Hash(h) => {
//                     let (value, value_len) = SetHashSnapshot(h);
//                     let t = b"hash";

//                     SaveHashSnapshot(
//                         key,
//                         &key_len,
//                         t,
//                         value,
//                         &value_len,
//                     )?;
//                 }

//                 RedisValue::Set(s) => {
//                     let (value, value_len) = SetSetSnapShot(s);
//                     let t = b"set";

//                     SaveSetSnapShot(
//                         key,
//                         &key_len,
//                         t,
//                         value,
//                         &value_len,
//                     )?;
//                 }
//             }
//         }

//         Ok(())
//     }

//     async fn ReadSnapShot(&mut self) -> Result<(), ServerError> {
//         let mut file = OpenOptions::new()
//             .read(true)
//             .open("redis.snapshot")
//             .map_err(ServerError::IoErr)?;

//         loop {
//             // Read key length
//             let mut key_len_buf = [0u8; 8];

//             match file.read_exact(&mut key_len_buf) {
//                 Ok(()) => {}

//                 Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
//                     // Clean EOF
//                     break;
//                 }

//                 Err(e) => {
//                     return Err(ServerError::IoErr(e));
//                 }
//             }

//             let key = ReadBytesFromSnapshot(key_len_buf, &mut file)?;

//             // Read type
//             let mut type_len_buf = [0u8; 8];

//             file.read_exact(&mut type_len_buf)
//                 .map_err(|e| {
//                     ServerError::SnapshotCorrupted(
//                         format!("Failed to read type length: {}", e)
//                     )
//                 })?;

//             let type_bytes =
//                 ReadBytesFromSnapshot(type_len_buf, &mut file)?;

//             let t = String::from_utf8(type_bytes)
//                 .map_err(ServerError::InvalidUtf8)?;

//             match t.as_str() {
//                 "string" => {
//                     let mut value_len_buf = [0u8; 8];

//                     file.read_exact(&mut value_len_buf)
//                         .map_err(|e| {
//                             ServerError::SnapshotCorrupted(
//                                 format!(
//                                     "Failed to read string value length: {}",
//                                     e
//                                 )
//                             )
//                         })?;

//                     let value =
//                         ReadBytesFromSnapshot(value_len_buf, &mut file)?;

//                     let redis_value =
//                         RedisValue::String(
//                             RedisString::new(value)
//                         );

//                     self.data.write().await.insert(key, redis_value);
//                 }

//                 "list" => {
//                     let mut value_len_buf = [0u8; 8];

//                     file.read_exact(&mut value_len_buf)
//                         .map_err(|e| {
//                             ServerError::SnapshotCorrupted(
//                                 format!(
//                                     "Failed to read list value length: {}",
//                                     e
//                                 )
//                             )
//                         })?;

//                     let value =
//                         ReadBytesFromSnapshot(value_len_buf, &mut file)?;

//                     let mut redis_list = RedisList::new();

//                     redis_list.replace(
//                         ReconstructListBytesFromBytes(value)?
//                     );

//                     let redis_value =
//                         RedisValue::List(redis_list);

//                     self.data.write().await.insert(key, redis_value);
//                 }

//                 "hash" => {
//                     let mut value_len_buf = [0u8; 8];

//                     file.read_exact(&mut value_len_buf)
//                         .map_err(|e| {
//                             ServerError::SnapshotCorrupted(
//                                 format!(
//                                     "Failed to read hash value length: {}",
//                                     e
//                                 )
//                             )
//                         })?;

//                     let value =
//                         ReadBytesFromSnapshot(value_len_buf, &mut file)?;

//                     let hash_data =
//                         ReconstructHashBytesFromBytes(value)?;

//                     let mut hash = RedisHash::new();

//                     hash.replace(hash_data);

//                     let redis_value =
//                         RedisValue::Hash(hash);

//                     self.data.write().await.insert(key, redis_value);
//                 }

//                 "set" => {
//                     let mut value_len_buf = [0u8; 8];

//                     file.read_exact(&mut value_len_buf)
//                         .map_err(|e| {
//                             ServerError::SnapshotCorrupted(
//                                 format!(
//                                     "Failed to read set value length: {}",
//                                     e
//                                 )
//                             )
//                         })?;

//                     let value =
//                         ReadBytesFromSnapshot(value_len_buf, &mut file)?;

//                     let set_data =
//                         ReconstructSetBytesFromBytes(value)?;

//                     let mut set = RedisSet::new();

//                     set.replace(set_data);

//                     let redis_value =
//                         RedisValue::Set(set);

//                     self.data.write().await.insert(key, redis_value);
//                 }

//                 _ => {
//                     return Err(
//                         ServerError::InvalidRedisType(t)
//                     );
//                 }
//             }
//         }

//         Ok(())
//     }
// }


// fn SetStringSnapshot(
//     redis_string: &RedisString
// ) -> (&[u8], [u8; 8]) {
//     let value = redis_string.get();
//     let value_len = value.len().to_be_bytes();

//     (value, value_len)
// }


// fn SetListSnapshot(
//     redis_list: &RedisList
// ) -> (&Vec<Vec<u8>>, [u8; 8]) {
//     let value = &redis_list.data;

//     let mut value_len = 0usize;

//     for element in value {
//         value_len += 8;
//         value_len += element.len();
//     }

//     (value, value_len.to_be_bytes())
// }


// fn SetHashSnapshot(
//     redis_hash: &RedisHash
// ) -> (&HashMap<Vec<u8>, Vec<u8>>, [u8; 8]) {
//     let value = &redis_hash.data;

//     let mut value_len = 0usize;

//     for (k, v) in value {
//         value_len += 8;
//         value_len += k.len();

//         value_len += 8;
//         value_len += v.len();
//     }

//     (value, value_len.to_be_bytes())
// }


// fn SetSetSnapShot(
//     redis_set: &RedisSet
// ) -> (&HashSet<Vec<u8>>, [u8; 8]) {
//     let value = &redis_set.data;

//     let mut value_len = 0usize;

//     for element in value {
//         value_len += 8;
//         value_len += element.len();
//     }

//     (value, value_len.to_be_bytes())
// }


// fn SaveStringSnapShot(
//     key: &[u8],
//     key_len: &[u8; 8],
//     t: &[u8],
//     value: &[u8],
//     value_len: &[u8; 8],
// ) -> Result<(), ServerError> {
//     let mut file = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .append(true)
//         .open("redis.snapshot")
//         .map_err(ServerError::IoErr)?;

//     let mut buf = Vec::new();

//     buf.extend_from_slice(key_len);
//     buf.extend_from_slice(key);

//     let type_len = t.len().to_be_bytes();

//     buf.extend_from_slice(&type_len);
//     buf.extend_from_slice(t);

//     buf.extend_from_slice(value_len);
//     buf.extend_from_slice(value);

//     file.write_all(&buf)
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }


// fn SaveListSnapShot(
//     key: &[u8],
//     key_len: &[u8; 8],
//     t: &[u8],
//     value: &[Vec<u8>],
//     value_len: &[u8; 8],
// ) -> Result<(), ServerError> {
//     let mut file = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .append(true)
//         .open("redis.snapshot")
//         .map_err(ServerError::IoErr)?;

//     let mut buf = Vec::new();

//     buf.extend_from_slice(key_len);
//     buf.extend_from_slice(key);

//     let type_len = t.len().to_be_bytes();

//     buf.extend_from_slice(&type_len);
//     buf.extend_from_slice(t);

//     buf.extend_from_slice(value_len);

//     for element in value {
//         let element_len = element.len().to_be_bytes();

//         buf.extend_from_slice(&element_len);
//         buf.extend_from_slice(element);
//     }

//     file.write_all(&buf)
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }


// fn SaveHashSnapshot(
//     key: &[u8],
//     key_len: &[u8; 8],
//     t: &[u8],
//     value: &HashMap<Vec<u8>, Vec<u8>>,
//     value_len: &[u8; 8],
// ) -> Result<(), ServerError> {
//     let mut file = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .append(true)
//         .open("redis.snapshot")
//         .map_err(ServerError::IoErr)?;

//     let mut buf = Vec::new();

//     buf.extend_from_slice(key_len);
//     buf.extend_from_slice(key);

//     let type_len = t.len().to_be_bytes();

//     buf.extend_from_slice(&type_len);
//     buf.extend_from_slice(t);

//     buf.extend_from_slice(value_len);

//     for (k, v) in value {
//         let key_len = k.len().to_be_bytes();

//         buf.extend_from_slice(&key_len);
//         buf.extend_from_slice(k);

//         let v_len = v.len().to_be_bytes();

//         buf.extend_from_slice(&v_len);
//         buf.extend_from_slice(v);
//     }

//     file.write_all(&buf)
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }


// fn SaveSetSnapShot(
//     key: &[u8],
//     key_len: &[u8; 8],
//     t: &[u8],
//     value: &HashSet<Vec<u8>>,
//     value_len: &[u8; 8],
// ) -> Result<(), ServerError> {
//     let mut file = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .append(true)
//         .open("redis.snapshot")
//         .map_err(ServerError::IoErr)?;

//     let mut buf = Vec::new();

//     buf.extend_from_slice(key_len);
//     buf.extend_from_slice(key);

//     let type_len = t.len().to_be_bytes();

//     buf.extend_from_slice(&type_len);
//     buf.extend_from_slice(t);

//     buf.extend_from_slice(value_len);

//     for element in value {
//         let element_len = element.len().to_be_bytes();

//         buf.extend_from_slice(&element_len);
//         buf.extend_from_slice(element);
//     }

//     file.write_all(&buf)
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }


// fn TruncateOldSnapshot() -> Result<(), ServerError> {
//     OpenOptions::new()
//         .create(true)
//         .write(true)
//         .truncate(true)
//         .open("redis.snapshot")
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }


// fn ReadBytesFromSnapshot(
//     len_buf: [u8; 8],
//     file: &mut File,
// ) -> Result<Vec<u8>, ServerError> {
//     let len = usize::from_be_bytes(len_buf);

//     let mut buf = vec![0u8; len];

//     file.read_exact(&mut buf)
//         .map_err(|e| {
//             ServerError::SnapshotCorrupted(
//                 format!(
//                     "Snapshot ended while reading {} bytes: {}",
//                     len, e
//                 )
//             )
//         })?;

//     Ok(buf)
// }


// fn ReconstructListBytesFromBytes(
//     value: Vec<u8>
// ) -> Result<Vec<Vec<u8>>, ServerError> {
//     let mut list = Vec::new();
//     let mut position = 0;

//     while position < value.len() {
//         if value.len() - position < 8 {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "List element length is incomplete".to_string()
//                 )
//             );
//         }

//         let mut len_buf = [0u8; 8];

//         len_buf.copy_from_slice(
//             &value[position..position + 8]
//         );

//         position += 8;

//         let element_len = usize::from_be_bytes(len_buf);

//         if element_len > value.len() - position {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "List element exceeds snapshot data".to_string()
//                 )
//             );
//         }

//         let element =
//             value[position..position + element_len].to_vec();

//         position += element_len;

//         list.push(element);
//     }

//     Ok(list)
// }


// fn ReconstructHashBytesFromBytes(
//     value: Vec<u8>
// ) -> Result<HashMap<Vec<u8>, Vec<u8>>, ServerError> {
//     let mut map = HashMap::new();
//     let mut position = 0;

//     while position < value.len() {
//         // Key length
//         if value.len() - position < 8 {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Hash key length is incomplete".to_string()
//                 )
//             );
//         }

//         let mut key_len_buf = [0u8; 8];

//         key_len_buf.copy_from_slice(
//             &value[position..position + 8]
//         );

//         position += 8;

//         let key_len = usize::from_be_bytes(key_len_buf);

//         // Key
//         if key_len > value.len() - position {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Hash key exceeds snapshot data".to_string()
//                 )
//             );
//         }

//         let key =
//             value[position..position + key_len].to_vec();

//         position += key_len;

//         // Value length
//         if value.len() - position < 8 {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Hash value length is incomplete".to_string()
//                 )
//             );
//         }

//         let mut value_len_buf = [0u8; 8];

//         value_len_buf.copy_from_slice(
//             &value[position..position + 8]
//         );

//         position += 8;

//         let value_len = usize::from_be_bytes(value_len_buf);

//         // Value
//         if value_len > value.len() - position {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Hash value exceeds snapshot data".to_string()
//                 )
//             );
//         }

//         let hash_value =
//             value[position..position + value_len].to_vec();

//         position += value_len;

//         map.insert(key, hash_value);
//     }

//     Ok(map)
// }


// fn ReconstructSetBytesFromBytes(
//     value: Vec<u8>
// ) -> Result<HashSet<Vec<u8>>, ServerError> {
//     let mut set = HashSet::new();
//     let mut position = 0;

//     while position < value.len() {
//         if value.len() - position < 8 {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Set element length is incomplete".to_string()
//                 )
//             );
//         }

//         let mut len_buf = [0u8; 8];

//         len_buf.copy_from_slice(
//             &value[position..position + 8]
//         );

//         position += 8;

//         let element_len = usize::from_be_bytes(len_buf);

//         if element_len > value.len() - position {
//             return Err(
//                 ServerError::SnapshotCorrupted(
//                     "Set element exceeds snapshot data".to_string()
//                 )
//             );
//         }

//         let element =
//             value[position..position + element_len].to_vec();

//         position += element_len;

//         set.insert(element);
//     }

//     Ok(set)
// }