use std::thread;

use crate::error::ServerError::{self, EnvErr, IoErr};

mod server;
pub mod error;
mod client;
mod cli;
mod benchmark;
#[tokio::main]
async fn main() {
    println!("Hello, world!");
    dotenvy::dotenv().ok();
    let (tx,tr)=tokio::sync::oneshot::channel::<u8>();

    //server setup
    tokio::spawn(async {
        match server::init::Init(tx).await {
            Ok(())=>{

            }
            Err(e)=>{
                match e {
                    error::ServerError::EnvErr(e) => {
                        eprintln!("Server environment error: {}", e);
                    }

                    error::ServerError::IoErr(e) => {
                        eprintln!("Server IO error: {}", e);
                    }
                    _=>{
                        eprintln!("Server error: {}", e);
                    }
                }
            }
        }
    });

  
    match tr.await {
        Ok(_) => {
            println!("Server started");
        }

        Err(_) => {
            eprintln!("Server failed to start");
            return;
        }
    }

    match client::init::Init() {
        Ok((mut redis))=>{
            println!("Server:Acquried hte cleint connection");

            cli::run(&mut redis);   
            // match benchmark::benchmark(&mut redis) {
            // Ok(()) => {
            //     println!("Benchmark completed successfully");
            // }
            // Err(e) => {
            //     eprintln!("Benchmark failed: {}", e);
            // }
            //  }

        }
        Err(e)=>{
            match e {
                IoErr(e)=>{
                    println!("Client: {}",e);
                }
                EnvErr(e)=>{
                    println!("Client: {}",e);
                }
                _=>{
                        
                }
            }
        }
    }

    
}
