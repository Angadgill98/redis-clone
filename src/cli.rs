use std::io::{self, Write};

use crate::client::init::redis_client;

pub fn run(redis: &mut redis_client) {
    println!("Supported commands:");
    println!("STRING: SET GET APPEND LEN");
    println!("LIST: LCREATE LPUSH RPUSH LPOP RPOP LLEN LINDEX LSET LCLEAR");
    println!("HASH: HCREATE HSET HGET HEXISTS HDEL HLEN HCLEAR HKEYS HVALUES");
    println!("SET: SCREATE SADD SREM SCONTAINS SLEN SCLEAR SVALUES");
    println!("Type commands below:");

    loop {
        print!("redis> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();

        io::stdin()
            .read_line(&mut input)
            .unwrap();

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();

        match parts[0].to_lowercase().as_str() {

            // ========================================================
            // STRING
            // ========================================================

            "set" => {
                if parts.len() != 3 {
                    println!("Usage: SET key value");
                    continue;
                }

                match redis.set(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "get" => {
                if parts.len() != 2 {
                    println!("Usage: GET key");
                    continue;
                }

                match redis.get(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "append" => {
                if parts.len() != 3 {
                    println!("Usage: APPEND key value");
                    continue;
                }

                match redis.append(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "len" => {
                if parts.len() != 2 {
                    println!("Usage: LEN key");
                    continue;
                }

                match redis.len(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            // ========================================================
            // LIST
            // ========================================================

            "lcreate" => {
                if parts.len() != 2 {
                    println!("Usage: LCREATE key");
                    continue;
                }

                match redis.lcreate(parts[1].to_string(), String::from("")) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lpush" => {
                if parts.len() != 3 {
                    println!("Usage: LPUSH key value");
                    continue;
                }

                match redis.lpush(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "rpush" => {
                if parts.len() != 3 {
                    println!("Usage: RPUSH key value");
                    continue;
                }

                match redis.rpush(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lpop" => {
                if parts.len() != 2 {
                    println!("Usage: LPOP key");
                    continue;
                }

                match redis.lpop(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "rpop" => {
                if parts.len() != 2 {
                    println!("Usage: RPOP key");
                    continue;
                }

                match redis.rpop(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "llen" => {
                if parts.len() != 2 {
                    println!("Usage: LLEN key");
                    continue;
                }

                match redis.llen(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lindex" => {
                if parts.len() != 3 {
                    println!("Usage: LINDEX key index");
                    continue;
                }

                let index = match parts[2].parse::<usize>() {
                    Ok(index) => index,
                    Err(_) => {
                        println!("(error) index must be a number");
                        continue;
                    }
                };

                match redis.lindex(parts[1].to_string(), index) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lset" => {
                if parts.len() != 4 {
                    println!("Usage: LSET key index value");
                    continue;
                }

                let index = match parts[2].parse::<usize>() {
                    Ok(index) => index,
                    Err(_) => {
                        println!("(error) index must be a number");
                        continue;
                    }
                };

                match redis.lset(
                    parts[1].to_string(),
                    index,
                    parts[3].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lclear" => {
                if parts.len() != 2 {
                    println!("Usage: LCLEAR key");
                    continue;
                }

                match redis.lclear(parts[1].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            // ========================================================
            // HASH
            // ========================================================

            "hcreate" => {
                if parts.len() != 2 {
                    println!("Usage: HCREATE key");
                    continue;
                }

                match redis.hcreate(parts[1].to_string(),String::from("")) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hset" => {
                if parts.len() != 4 {
                    println!("Usage: HSET key field value");
                    continue;
                }

                match redis.hset(
                    parts[1].to_string(),
                    parts[2].to_string(),
                    parts[3].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hget" => {
                if parts.len() != 3 {
                    println!("Usage: HGET key field");
                    continue;
                }

                match redis.hget(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hexists" => {
                if parts.len() != 3 {
                    println!("Usage: HEXISTS key field");
                    continue;
                }

                match redis.hexists(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hdel" => {
                if parts.len() != 3 {
                    println!("Usage: HDEL key field");
                    continue;
                }

                match redis.hdel(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hlen" => {
                if parts.len() != 2 {
                    println!("Usage: HLEN key");
                    continue;
                }

                match redis.hlen(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hclear" => {
                if parts.len() != 2 {
                    println!("Usage: HCLEAR key");
                    continue;
                }

                match redis.hclear(parts[1].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hkeys" => {
                if parts.len() != 2 {
                    println!("Usage: HKEYS key");
                    continue;
                }

                match redis.hkeys(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hvalues" => {
                if parts.len() != 2 {
                    println!("Usage: HVALUES key");
                    continue;
                }

                match redis.hvalues(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            // ========================================================
            // SET
            // ========================================================

            "screate" => {
                if parts.len() != 2 {
                    println!("Usage: SCREATE key");
                    continue;
                }

                match redis.screate(parts[1].to_string(), String::from("")) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "sadd" => {
                if parts.len() != 3 {
                    println!("Usage: SADD key value");
                    continue;
                }

                match redis.sadd(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "srem" => {
                if parts.len() != 3 {
                    println!("Usage: SREM key value");
                    continue;
                }

                match redis.srem(parts[1].to_string(), parts[2].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "scontains" => {
                if parts.len() != 3 {
                    println!("Usage: SCONTAINS key value");
                    continue;
                }

                match redis.scontains(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "slen" => {
                if parts.len() != 2 {
                    println!("Usage: SLEN key");
                    continue;
                }

                match redis.slen(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "sclear" => {
                if parts.len() != 2 {
                    println!("Usage: SCLEAR key");
                    continue;
                }

                match redis.sclear(parts[1].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "svalues" => {
                if parts.len() != 2 {
                    println!("Usage: SVALUES key");
                    continue;
                }

                match redis.svalues(parts[1].to_string()) {
                    Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
                    Err(e) => println!("(error) {}", e),
                }
            }

            // ========================================================
            // EXIT
            // ========================================================

            "exit" | "quit" => {
                println!("Bye!");
                break;
            }

            _ => {
                println!("Unknown command: {}", parts[0]);
            }
        }
    }
}