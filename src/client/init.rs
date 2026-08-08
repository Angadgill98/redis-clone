use std::{io::Write, net::TcpStream};

use crate::error::ServerError;



pub fn Init()->Result<(),ServerError>{
    let socket=CreateSocket()?;

    let redis=redis_client::new(socket);

    Ok(())
}

fn CreateSocket()->Result<std::net::TcpStream, ServerError>{
    let server_addr=std::env::var("redis_server_addr")?;

    let socket=std::net::TcpStream::connect(server_addr)?;

    Ok(socket)
}

struct redis_client{
    socket:TcpStream
}

impl redis_client {
    fn new(stream:TcpStream)->Self{
        redis_client { 
            socket: stream 
        }
    }

    fn set(&mut self,key:String,value:String){
        let key_bytes=key.as_bytes();
        let key_len=key_bytes.len().to_be_bytes();

        let value_bytes=value.as_bytes();
        let value_len=value_bytes.len().to_be_bytes();

        let op=1;
        

        self.socket.write_all(&[op]).unwrap();
        self.socket.write_all(&key_len).unwrap();
        self.socket.write_all(key_bytes).unwrap();
        self.socket.write_all(&value_len).unwrap();
        self.socket.write_all(value_bytes).unwrap();

    }

    fn get(&mut self,key:String){
        let key_bytes=key.as_bytes();
        let key_len=key_bytes.len().to_be_bytes();

        let op=1;
        

        self.socket.write_all(&[op]).unwrap();
        self.socket.write_all(&key_len).unwrap();
        self.socket.write_all(key_bytes).unwrap();
    }
}