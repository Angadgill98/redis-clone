use std::thread;

use crate::error::ServerError::{self, EnvErr, IoErr};

mod server;
pub mod error;
mod client;


#[tokio::main]
async fn main() {
    println!("Hello, world!");

    //server setup
    tokio::spawn(async {
        match server::init::Init().await {
            Ok(())=>{

            }
            Err(e)=>{
                match e {
                    error::ServerError::EnvErr(e)=>{

                    }
                    error::ServerError::IoErr(e)=>{

                    }
                    _=>{

                    }
                }
            }
        }
    });

  
    

    match client::init::Init() {
        Ok(())=>{

        }
        Err(e)=>{
            match e {
                IoErr(e)=>{

                }
                EnvErr(e)=>{

                }
                _=>{
                        
                }
            }
        }
    }

    
}
