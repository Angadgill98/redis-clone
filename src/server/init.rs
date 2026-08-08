
use std::{collections::hash_map, net::{TcpListener, TcpStream}};

use crate::error::ServerError;



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
    Strings(String),
    Number(u128)
}

struct redis_server{
    socket:TcpStream,
    data:hash_map::HashMap<String,RedisValues>
}

impl redis_server {
    fn new(socket:TcpStream)->Self{
        Self { 
            socket: socket,
            data: hash_map::HashMap::new()
        }
    }
    fn set(key:String,value:String){

    }
}