
use std::{ sync::{Arc, Mutex}};

use tokio::{io::AsyncReadExt, net::{TcpListener, TcpStream}};

use crate::{error::ServerError, server::redis::{self, RedisList, RedisServer, RedisString, RedisValue}};



pub async fn Init()->Result<(),ServerError>{
    let socket = CreateSocket().await?;
    
    let redis=Arc::new(Mutex::new(RedisServer::new())) ;

    
    loop {
        let (stream,client_addr)=socket.accept().await?;
        let redis_thread=Arc::clone(&redis);
        tokio::spawn(async move{
            HandleClient(redis_thread,stream,client_addr);        

        });
    }
    Ok(())
}

async fn CreateSocket()->Result<TcpListener,ServerError>{
    let addr=std::env::var("redis_server_addr")?;

    let socket =tokio::net::TcpListener::bind(addr).await?;

    Ok(socket)
}

fn HandleClient(redis:Arc<Mutex<RedisServer>>,mut stream:TcpStream,client_addr:core::net::SocketAddr){
    loop {
        let mut buf=[0u8;65000];
        stream.read(&mut buf);


        let op_no=buf[0];
        let len=buf[1] as usize;

        let operation=&buf[2..2+len];
        let (redis_type,command)=Simplify(operation).unwrap();

        HandleType(&redis,redis_type,operation, command);

        //HandleCommands(operation, command, args_len);

    }
}

fn Simplify(operation:&[u8])->Result<(String,String),std::string::FromUtf8Error>{
    let redis_type_len=operation[0] as usize;
    let redis_type=String::from_utf8(operation[1..1+redis_type_len].to_vec()).unwrap();

    let new_start=1+redis_type_len;

    let command_len=operation[new_start] as usize;
    let new_start=new_start+1;

    let command=String::from_utf8(operation[new_start..new_start+command_len].to_vec())?;


    Ok((redis_type,command))

}

fn HandleType(redis:&Arc<Mutex<RedisServer>>,redis_type:String,operation:&[u8],command:String){
    match redis_type.trim() {
        "string"=>{
            HandleStringOp(redis, command, String::from("string"));
        }
        "list"=>{

        }
        _=>{

        }
    }
}

fn HandleStringOp(redis:&Arc<Mutex<RedisServer>>,command:String,t:String){
    let mut command:Vec<Vec<u8>>=command.split_whitespace()
        .map(|word| word.as_bytes().to_vec())
        .collect();
    
    match std::str::from_utf8(&command[0]).unwrap(){
        "set"=>{
            let key=command.remove(1);
            let value=command.remove(1);
            
            let mut redis= redis.lock().unwrap();
            redis.create_string(key, value);
            drop(redis);
            
        }   
        "get"=>{
            let key=command.remove(1);
            let mut redis= redis.lock().unwrap();

            let redis_value=redis.data.get_mut(&key).unwrap();

            let redis_string=GetRedisSting(redis_value).unwrap();

            let value =redis_string.get();
        }
        "append"=>{
            let key=command.remove(1);
            let append=command.remove(1);
            let mut redis= redis.lock().unwrap();

            let redis_value=redis.data.get_mut(&key).unwrap();

            let redis_string=GetRedisSting(redis_value).unwrap();

            redis_string.append(&append);

        }
        "len"=>{
            let key=command.remove(1);
            
            let mut redis= redis.lock().unwrap();

            let redis_value=redis.data.get_mut(&key).unwrap();

            let redis_string=GetRedisSting(redis_value).unwrap();

            let len=redis_string.len();
        }
        _=>{

        }
    }
}

fn GetRedisSting(value: &mut RedisValue,) -> Option<&mut RedisString> {
    match value {
        RedisValue::String(string) => Some(string),
        _ => None,
    }
}



fn HandleListOp(redis:&Arc<Mutex<RedisServer>>,command:String,t:String){
    let mut command:Vec<Vec<u8>>=command.split_whitespace()
    .map(|word| word.as_bytes().to_vec())
    .collect();
    
    match std::str::from_utf8(&command[0]).unwrap(){
        "lcreate"=>{
            let key=command.remove(1);
            let mut redis= redis.lock().unwrap();
            redis.create_list(key);

        }
        "lpush"=>{
            let key=command.remove(1);
            let value=command.remove(1);
            
            let mut redis= redis.lock().unwrap();
            
            let redis_value=redis.data.get_mut(&key).unwrap();

            let redis_list=GetRedisList(redis_value).unwrap();

            redis_list.push_front(value);

        }
        "rpush"=>{
            let key=command.remove(1);
            let value=command.remove(1);
            
            let mut redis= redis.lock().unwrap();
            
            let redis_value=redis.data.get_mut(&key).unwrap();

            let redis_list=GetRedisList(redis_value).unwrap();

            redis_list.push_back(value);
            
        }
        _=>{

        }
    }
}

fn GetRedisList(value: &mut RedisValue,) -> Option<&mut RedisList> {
    match value {
        RedisValue::List(list) => Some(list),
        _ => None,
    }
}