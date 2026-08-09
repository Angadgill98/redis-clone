use std::thread;

use crate::error::ServerError::{self, EnvErr, IoErr};

mod server;
pub mod error;
mod client;
fn main() {
    println!("Hello, world!");

    let (tx,tr)=std::sync::mpsc::channel::<u8>();

    //server setup
    thread::spawn(move||{
        match server::init::Init(tx) {
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
    let isup=tr.recv().unwrap();
    match isup {
        1=>{

        }
        _=>{
            return;
        }
    }
    

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
