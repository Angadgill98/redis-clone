use std::{collections::HashMap, fs::OpenOptions};



pub trait Persistence {
    fn WriteToLog(&self,command:String,args:&[String]) {
        let content = format!("{} {}", command,args.join(" "));
        let file=std::fs::write("redis.log", content).unwrap();
        
    }
    fn ReadLog()->String {
        let mut file=create_if_not_exists("refis.log").unwrap();
        let data=std::fs::read("redis.log").unwrap();
        let content=String::from_utf8(data).unwrap();
        content
    }

    fn ReconstructLogFile(content:String)->HashMap<Vec<u8>,Vec<u8>>{
        let mut map=HashMap::new();

        for operation in content.lines(){
            let commands:Vec<&str>=operation.split(" ").collect();
            match commands[0] {
                "set"=>{
                    let key=commands[1].as_bytes().to_vec();
                    let value=commands[2].as_bytes().to_vec();

                    map.insert(key, value);
                }
                "get"=>{}
                _=>{
                
                }
            }
        }

        map
    }

    fn Comapaction(){

    }

    fn SaveSnapShot(){

    }

    fn ReadSnapShot(){

    }
}

fn create_if_not_exists(path: &str) -> Result<std::fs::File,Box<dyn std::error::Error>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    Ok(file)
}