// use std::{io::{Read, Write}, net::TcpStream, sync::mpsc::{self, Receiver, Sender}};

// use crate::error::ServerError;



// pub fn Init()->Result<(redis_client),ServerError>{
//     let socket=CreateSocket()?;
//     let mut pubSub_socket_fix=socket.try_clone().unwrap();
//     let redis=redis_client::new(socket);

    
//     Ok(redis)
// }

// fn CreateSocket()->Result<std::net::TcpStream, ServerError>{
//     let server_addr=std::env::var("redis_server_addr")?;

//     let socket=std::net::TcpStream::connect(server_addr)?;

//     Ok(socket)
// }

// pub struct redis_client{
//     socket:TcpStream,
//     pub transaction_mode:bool,
//     pub transaction_queue:Vec<String>,
//     pub subscribed_mode:bool
// }

// impl redis_client {
//     fn new(stream: TcpStream) -> Self {
//         redis_client {
//             socket: stream,
//             transaction_mode:false,
//             transaction_queue:Vec::new(),

//             subscribed_mode:false
//         }
//     }
    // pub fn subscribe(&mut self,  channel: String) -> Result<(), ServerError>{
    //     let command=format!("subscribe {}",channel);
    //     let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", command.trim());
    //     SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
    //     Ok(())
    // }

    // pub fn publish(&mut self, channel: String, message: String) -> Result<(), ServerError>{
    //     let command=format!("publish {} {}",channel,message);

    //     let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", command.trim());
    //     SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
    //     Ok(())
    // }

    // pub fn unsubscribe(&mut self,channel: String) -> Result<(), ServerError>{
    //     let commadn=format!("publish {}",channel);
    //     let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", commadn.trim());
    //     SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
    //     Ok(())
    // }

//     pub fn HandleTransaction(&mut self)-> Result<(), ServerError>{

        
//         let commands=&self.transaction_queue;
//         let mut buf= Vec::new();
//         let t_bytes = "transaction".as_bytes();
//         let t_len=(t_bytes.len() as u64).to_be_bytes();

//         buf.extend_from_slice(&t_len);
//         buf.extend_from_slice(t_bytes);
//         let mut temp=Vec::new();
//         for cmd in commands {
//             let cmd=format!("{}\n",cmd);
//             let cmd=cmd.as_bytes();
//             temp.extend_from_slice(cmd);
//         }

//         let temp_len=(temp.len() as u64).to_be_bytes();
//         buf.extend_from_slice(&temp_len);
//         buf.extend_from_slice(&temp);

//         let buf_len=(buf.len() as u64).to_be_bytes();
//         let mut final_buf=Vec::new();
//         final_buf.extend_from_slice(&buf_len);
//         final_buf.extend_from_slice(&buf);

//         self.socket.write_all(&final_buf).map_err(ServerError::IoErr)?;

//         Ok(())
        
//     }

//     // ========================================================
//     // STRING
//     // ========================================================

//     pub fn set(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("string", &format!("set {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn get(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("string", &format!("get {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn append(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("string", &format!("append {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn len(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("string", &format!("len {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     // ========================================================
//     // LIST
//     // ========================================================

//     pub fn lcreate(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lcreate {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn lpush(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lpush {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn rpush(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("rpush {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn lpop(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lpop {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn rpop(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("rpop {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn llen(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("llen {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn lindex(&mut self, key: String, index: usize) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lindex {} {}", key, index));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn lset(&mut self, key: String, index: usize, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lset {} {} {}", key, index, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn lclear(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("list", &format!("lclear {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     // ========================================================
//     // HASH
//     // ========================================================

//     pub fn hcreate(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hcreate {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hset(&mut self, key: String, field: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hset {} {} {}", key, field, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hget(&mut self, key: String, field: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hget {} {}", key, field));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hexists(&mut self, key: String, field: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hexists {} {}", key, field));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hdel(&mut self, key: String, field: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hdel {} {}", key, field));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hlen(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hlen {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hclear(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hclear {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hkeys(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hkeys {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn hvalues(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("hash", &format!("hvalues {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     // ========================================================
//     // SET
//     // ========================================================

//     pub fn screate(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("screate {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn sadd(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("sadd {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn srem(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("srem {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn scontains(&mut self, key: String, value: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("scontains {} {}", key, value));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn slen(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("slen {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn sclear(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("sclear {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }

//     pub fn svalues(&mut self, key: String) -> Result<(), ServerError> {
//         let (t_bytes, t_len, data_bytes, data_len) =
//             CreateBuffer("set", &format!("svalues {}", key));

//         SendBuffer(
//             &mut self.socket,
//             &t_bytes,
//             &t_len,
//             &data_bytes,
//             &data_len,
//         )?;

//         Ok(())
//     }
// }

// // ========================================================
// // BUFFER CREATION
// // ========================================================

// fn CreateBuffer(
//     redis_type: &str,
//     command: &str,
// ) -> (Vec<u8>, [u8; 8], Vec<u8>, [u8; 8]) {
//     let t_bytes = redis_type.as_bytes().to_vec();

//     let t_len = (t_bytes.len() as u64).to_be_bytes();

//     let data_bytes = command.as_bytes().to_vec();

//     let data_len = (data_bytes.len() as u64).to_be_bytes();

//     (
//         t_bytes,
//         t_len,
//         data_bytes,
//         data_len,
//     )
// }

// // ========================================================
// // ERROR RESPONSE
// // ========================================================

// fn HandleError(socket: &mut TcpStream) -> Result<(), ServerError> {
//     let mut response_len = [0u8; 8];

//     socket
//         .read_exact(&mut response_len)
//         .map_err(ServerError::IoErr)?;

//     let response_len = u64::from_be_bytes(response_len) as usize;

//     let mut response = vec![0u8; response_len];

//     socket
//         .read_exact(&mut response)
//         .map_err(ServerError::IoErr)?;

//     let error = String::from_utf8(response)
//         .map_err(|e| ServerError::ServerResponse(e.to_string()))?;

//     Err(ServerError::ServerResponse(error))
// }

// // ========================================================
// // SEND REQUEST
// // ========================================================

// fn SendBuffer(
//     socket: &mut TcpStream,
//     t_bytes: &[u8],
//     t_len: &[u8; 8],
//     data_bytes: &[u8],
//     data_len: &[u8; 8],
// ) -> Result<(), ServerError> {
//     let mut buffer = Vec::new();

//     buffer.extend_from_slice(t_len);
//     buffer.extend_from_slice(t_bytes);

//     buffer.extend_from_slice(data_len);
//     buffer.extend_from_slice(data_bytes);

//     let buffer_len = (buffer.len() as u64).to_be_bytes();

//     let mut final_buffer = Vec::new();

//     final_buffer.extend_from_slice(&buffer_len);
//     final_buffer.extend_from_slice(&buffer);

//     socket
//         .write_all(&final_buffer)
//         .map_err(ServerError::IoErr)?;

//     Ok(())
// }

// // ========================================================
// // READ RESPONSE WITH VALUE
// // ========================================================

// fn ReadStatus(socket: &mut TcpStream) -> Result<Vec<u8>, ServerError> {
//     let mut status_buf = [0u8; 1];

//     socket
//         .read_exact(&mut status_buf)
//         .map_err(ServerError::IoErr)?;

//     match status_buf[0] {
//         1 => {
//             let mut len_buf = [0u8; 8];

//             socket
//                 .read_exact(&mut len_buf)
//                 .map_err(ServerError::IoErr)?;

//             let len = u64::from_be_bytes(len_buf) as usize;

//             let mut response = vec![0u8; len];

//             socket
//                 .read_exact(&mut response)
//                 .map_err(ServerError::IoErr)?;

//             Ok(response)
//         }

//         0 => {
//             HandleError(socket)?;
//             unreachable!()
//         }

//         status => {
//             Err(ServerError::ServerResponse(
//                 format!("Invalid status code: {}", status),
//             ))
//         }
//     }
// }

// // ========================================================
// // READ RESPONSE WITHOUT VALUE
// // ========================================================

// fn ReadStatusNoResponse(socket: &mut TcpStream) -> Result<(), ServerError> {
//     let mut status_buf = [0u8; 1];

//     socket
//         .read_exact(&mut status_buf)
//         .map_err(ServerError::IoErr)?;

//     match status_buf[0] {
//         1 => Ok(()),

//         0 => HandleError(socket),

//         status => {
//             Err(ServerError::ServerResponse(
//                 format!("Invalid status code: {}", status),
//             ))
//         }
//     }
// }


use std::{io::{Read, Write}, net::TcpStream, thread};

use tokio::sync::oneshot::{self, Receiver};

use crate::error::ServerError;



pub fn Init()->Result<redis_client,ServerError>{
    let socket=CreateSocket()?;

    let redis=redis_client::new(socket);

    Ok(redis)
}

fn CreateSocket()->Result<std::net::TcpStream, ServerError>{
    let server_addr=std::env::var("redis_server_addr")?;

    let socket=std::net::TcpStream::connect(server_addr)?;

    Ok(socket)
}

pub struct redis_client{
    socket:TcpStream,
    pub transaction_mode:bool,
    pub transaction_queue:Vec<String>,

    pub subscription_mode:bool,
    pub thread_handler:Option<oneshot::Sender<u8>>
}

impl redis_client {
    fn new(stream: TcpStream) -> Self {
        redis_client {
            socket: stream,
            transaction_mode:false,
            transaction_queue:Vec::new(),
            subscription_mode:false,
            thread_handler:None
        }
    }
    pub fn subscribe(&mut self,  channel: String) -> Result<(), ServerError>{
        
        let command=format!("subscribe {}",channel);
        let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", command.trim());
        SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
        self.subscription_mode=true;
        let (kill_tx, mut kill_rx)  = oneshot::channel::<u8>();
        self.thread_handler = Some(kill_tx);
        self.StartBackgroundReader(kill_rx,self.socket.try_clone().unwrap());
        Ok(())
    }
    fn StartBackgroundReader(&self,mut kill_rx:oneshot::Receiver<u8>,reader:TcpStream) {
        

        tokio::spawn(async move {
            loop {
                tokio::select! {

                    // ==============================
                    // KILL SIGNAL
                    // ==============================
                    _ = &mut kill_rx => {
                        println!("Subscription task stopped");
                        break;
                    }

                    // ==============================
                    // READ SUBSCRIPTION RESPONSE
                    // ==============================
                    result = tokio::task::spawn_blocking({

                        let mut reader = reader.try_clone().unwrap();

                        move || {

                            // --------------------------
                            // Read response type
                            // --------------------------
                            let mut res_with_data = [0u8; 1];

                            reader.read_exact(&mut res_with_data)?;

                            // --------------------------
                            // Read status
                            // --------------------------
                            let mut status = [0u8; 1];

                            reader.read_exact(&mut status)?;

                            // --------------------------
                            // Read response length
                            // --------------------------
                            let mut response_len = [0u8; 8];

                            reader.read_exact(&mut response_len)?;

                            let response_len =
                                u64::from_be_bytes(response_len) as usize;

                            // --------------------------
                            // Read actual value
                            // --------------------------
                            let mut value = vec![0u8; response_len];

                            reader.read_exact(&mut value)?;

                            Ok::<Vec<u8>, std::io::Error>(value)
                        }
                    }) => {

                        match result {

                            Ok(Ok(value)) => {
                                println!(
                                    "Subscription message: {:?}",
                                    value
                                );

                                match String::from_utf8(value) {

                                    Ok(value) => {
                                        println!("{}", value);
                                    }

                                    Err(_) => {
                                        println!("Received binary data");
                                    }
                                }
                            }

                            Ok(Err(e)) => {
                                println!(
                                    "Subscription reader stopped: {}",
                                    e
                                );
                                break;
                            }

                            Err(e) => {
                                println!(
                                    "Subscription task failed: {}",
                                    e
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn publish(&mut self, channel: String, message: String) -> Result<(), ServerError>{
        let command=format!("publish {} {}",channel,message);
        let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", command.trim());
        SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
        
        Ok(())
    }

    pub fn unsubscribe(&mut self,channel: String) -> Result<(), ServerError>{
        let commadn=format!("publish {}",channel);
        let (t_bytes, t_len, data_bytes, data_len)=CreateBuffer("pubSub", commadn.trim());
        SendBuffer(&mut self.socket, &t_bytes, &t_len, &data_bytes, &data_len)?;
        self.subscription_mode=false;
        if let Some(kill_tx) = self.thread_handler.take() {
            let _ = kill_tx.send(1);
        }
        Ok(())
    }
    pub fn HandleTransaction(&mut self)-> Result<Vec<u8>, ServerError>{

        
        let commands=&self.transaction_queue;
        let mut buf= Vec::new();
        let t_bytes = "transaction".as_bytes();
        let t_len=(t_bytes.len() as u64).to_be_bytes();

        buf.extend_from_slice(&t_len);
        buf.extend_from_slice(t_bytes);
        let mut temp=Vec::new();
        for cmd in commands {
            let cmd=format!("{}\n",cmd);
            let cmd=cmd.as_bytes();
            temp.extend_from_slice(cmd);
        }

        let temp_len=(temp.len() as u64).to_be_bytes();
        buf.extend_from_slice(&temp_len);
        buf.extend_from_slice(&temp);

        let buf_len=(buf.len() as u64).to_be_bytes();
        let mut final_buf=Vec::new();
        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        self.socket.write_all(&final_buf).map_err(ServerError::IoErr)?;

        ReadStatus(&mut self.socket)
        
    }

    // ========================================================
    // STRING
    // ========================================================

    pub fn set(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("string", &format!("set {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn get(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("string", &format!("get {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn append(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("string", &format!("append {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn len(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("string", &format!("len {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    // ========================================================
    // LIST
    // ========================================================

    pub fn lcreate(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lcreate {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn lpush(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lpush {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn rpush(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("rpush {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn lpop(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lpop {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn rpop(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("rpop {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn llen(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("llen {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn lindex(&mut self, key: String, index: usize) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lindex {} {}", key, index));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn lset(&mut self, key: String, index: usize, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lset {} {} {}", key, index, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn lclear(&mut self, key: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("list", &format!("lclear {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    // ========================================================
    // HASH
    // ========================================================

    pub fn hcreate(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hcreate {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn hset(&mut self, key: String, field: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hset {} {} {}", key, field, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn hget(&mut self, key: String, field: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hget {} {}", key, field));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn hexists(&mut self, key: String, field: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hexists {} {}", key, field));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn hdel(&mut self, key: String, field: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hdel {} {}", key, field));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn hlen(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hlen {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn hclear(&mut self, key: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hclear {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn hkeys(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hkeys {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn hvalues(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("hash", &format!("hvalues {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    // ========================================================
    // SET
    // ========================================================

    pub fn screate(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("screate {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn sadd(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("sadd {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn srem(&mut self, key: String, value: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("srem {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn scontains(&mut self, key: String, value: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("scontains {} {}", key, value));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn slen(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("slen {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }

    pub fn sclear(&mut self, key: String) -> Result<(), ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("sclear {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatusNoResponse(&mut self.socket)
    }

    pub fn svalues(&mut self, key: String) -> Result<Vec<u8>, ServerError> {
        let (t_bytes, t_len, data_bytes, data_len) =
            CreateBuffer("set", &format!("svalues {}", key));

        SendBuffer(
            &mut self.socket,
            &t_bytes,
            &t_len,
            &data_bytes,
            &data_len,
        )?;

        ReadStatus(&mut self.socket)
    }
}

// ========================================================
// BUFFER CREATION
// ========================================================

fn CreateBuffer(
    redis_type: &str,
    command: &str,
) -> (Vec<u8>, [u8; 8], Vec<u8>, [u8; 8]) {
    let t_bytes = redis_type.as_bytes().to_vec();

    let t_len = (t_bytes.len() as u64).to_be_bytes();

    let data_bytes = command.as_bytes().to_vec();

    let data_len = (data_bytes.len() as u64).to_be_bytes();

    (
        t_bytes,
        t_len,
        data_bytes,
        data_len,
    )
}

// ========================================================
// ERROR RESPONSE
// ========================================================

fn HandleError(socket: &mut TcpStream) -> Result<(), ServerError> {
    let mut response_len = [0u8; 8];

    socket
        .read_exact(&mut response_len)
        .map_err(ServerError::IoErr)?;

    let response_len = u64::from_be_bytes(response_len) as usize;

    let mut response = vec![0u8; response_len];

    socket
        .read_exact(&mut response)
        .map_err(ServerError::IoErr)?;

    let error = String::from_utf8(response)
        .map_err(|e| ServerError::ServerResponse(e.to_string()))?;

    Err(ServerError::ServerResponse(error))
}

// ========================================================
// SEND REQUEST
// ========================================================

fn SendBuffer(
    socket: &mut TcpStream,
    t_bytes: &[u8],
    t_len: &[u8; 8],
    data_bytes: &[u8],
    data_len: &[u8; 8],
) -> Result<(), ServerError> {
    let mut buffer = Vec::new();

    buffer.extend_from_slice(t_len);
    buffer.extend_from_slice(t_bytes);

    buffer.extend_from_slice(data_len);
    buffer.extend_from_slice(data_bytes);

    let buffer_len = (buffer.len() as u64).to_be_bytes();

    let mut final_buffer = Vec::new();

    final_buffer.extend_from_slice(&buffer_len);
    final_buffer.extend_from_slice(&buffer);

    socket
        .write_all(&final_buffer)
        .map_err(ServerError::IoErr)?;

    Ok(())
}

// ========================================================
// READ RESPONSE WITH VALUE
// ========================================================

fn ReadStatus(socket: &mut TcpStream) -> Result<Vec<u8>, ServerError> {
    let mut status_buf = [0u8; 1];

    socket
        .read_exact(&mut status_buf)
        .map_err(ServerError::IoErr)?;

    match status_buf[0] {
        1 => {
            let mut len_buf = [0u8; 8];

            socket
                .read_exact(&mut len_buf)
                .map_err(ServerError::IoErr)?;

            let len = u64::from_be_bytes(len_buf) as usize;

            let mut response = vec![0u8; len];

            socket
                .read_exact(&mut response)
                .map_err(ServerError::IoErr)?;

            Ok(response)
        }

        0 => {
            HandleError(socket)?;
            unreachable!()
        }

        status => {
            Err(ServerError::ServerResponse(
                format!("Invalid status code: {}", status),
            ))
        }
    }
}

// ========================================================
// READ RESPONSE WITHOUT VALUE
// ========================================================

fn ReadStatusNoResponse(socket: &mut TcpStream) -> Result<(), ServerError> {
    let mut status_buf = [0u8; 1];

    socket
        .read_exact(&mut status_buf)
        .map_err(ServerError::IoErr)?;

    match status_buf[0] {
        1 => Ok(()),

        0 => HandleError(socket),

        status => {
            Err(ServerError::ServerResponse(
                format!("Invalid status code: {}", status),
            ))
        }
    }
}
