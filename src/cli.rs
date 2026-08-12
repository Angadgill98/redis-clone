use std::io::{self, Write};

use crate::client::init::redis_client;

pub fn run(redis: &mut redis_client) {
    println!("Supported commands:");
    println!("STRING: SET GET APPEND LEN");
    println!("LIST: LCREATE LPUSH RPUSH LPOP RPOP LLEN LINDEX LSET LCLEAR");
    println!("HASH: HCREATE HSET HGET HEXISTS HDEL HLEN HCLEAR HKEYS HVALUES");
    println!("SET: SCREATE SADD SREM SCONTAINS SLEN SCLEAR SVALUES");
    println!("PUBSUB: SUBSCRIBE PUBLISH UNSUBSCRIBE");
    println!("TRANSACTION: MULTI EXEC DISCARD");
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

        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            // ========================================================
            // PUBSUB
            // ========================================================

            "subscribe" => {
                if parts.len() != 2 {
                    println!("Usage: SUBSCRIBE channel");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "pubsub".to_string(),
                    "subscribe".to_string(),
                ) {
                    continue;
                }

                match redis.subscribe(parts[1].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "publish" => {
                if parts.len() < 3 {
                    println!("Usage: PUBLISH channel message");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2..].join(" ")
                );

                if ModeHandler(
                    redis,
                    command,
                    "pubsub".to_string(),
                    "publish".to_string(),
                ) {
                    continue;
                }

                match redis.publish(
                    parts[1].to_string(),
                    parts[2..].join(" "),
                ) {
                    Ok(()) => {}
                    Err(e) => println!("(error) {}", e),
                }
            }

            "unsubscribe" => {
                if parts.len() != 2 {
                    println!("Usage: UNSUBSCRIBE channel");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "pubsub".to_string(),
                    "unsubscribe".to_string(),
                ) {
                    continue;
                }

                match redis.unsubscribe(parts[1].to_string()) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            // ========================================================
            // TRANSACTION
            // ========================================================

            "multi" => {
                if parts.len() != 1 {
                    println!("Usage: MULTI");
                    continue;
                }

                if redis.transaction_mode {
                    println!("(error) MULTI calls cannot be nested");
                    continue;
                }

                redis.transaction_mode = true;
                redis.transaction_queue.clear();

                println!("OK");
            }

            "exec" => {
                if parts.len() != 1 {
                    println!("Usage: EXEC");
                    continue;
                }

                if !redis.transaction_mode {
                    println!("(error) EXEC without MULTI");
                    continue;
                }

                match redis.HandleTransaction() {
                    Ok(res) => {
                        println!("{}", String::from_utf8_lossy(&res));
                        
                    }
                    Err(e) => println!("(error) {}", e),
                }

                redis.transaction_queue.clear();
                redis.transaction_mode = false;
            }

            "discard" => {
                if parts.len() != 1 {
                    println!("Usage: DISCARD");
                    continue;
                }

                if !redis.transaction_mode {
                    println!("(error) DISCARD without MULTI");
                    continue;
                }

                redis.transaction_queue.clear();
                redis.transaction_mode = false;

                println!("OK");
            }

            // ========================================================
            // STRING
            // ========================================================

            "set" => {
                if parts.len() != 3 {
                    println!("Usage: SET key value");
                    continue;
                }

                let command = format!("{} {}", parts[1], parts[2]);

                if ModeHandler(
                    redis,
                    command,
                    "string".to_string(),
                    "set".to_string(),
                ) {
                    continue;
                }

                match redis.set(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "get" => {
                if parts.len() != 2 {
                    println!("Usage: GET key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "string".to_string(),
                    "get".to_string(),
                ) {
                    continue;
                }

                match redis.get(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "append" => {
                if parts.len() != 3 {
                    println!("Usage: APPEND key value");
                    continue;
                }

                let command = format!("{} {}", parts[1], parts[2]);

                if ModeHandler(
                    redis,
                    command,
                    "string".to_string(),
                    "append".to_string(),
                ) {
                    continue;
                }

                match redis.append(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "len" => {
                if parts.len() != 2 {
                    println!("Usage: LEN key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "string".to_string(),
                    "len".to_string(),
                ) {
                    continue;
                }

                match redis.len(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lcreate".to_string(),
                ) {
                    continue;
                }

                match redis.lcreate(
                    parts[1].to_string(),
                    String::from(""),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lpush" => {
                if parts.len() != 3 {
                    println!("Usage: LPUSH key value");
                    continue;
                }

                let command = format!("{} {}", parts[1], parts[2]);

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lpush".to_string(),
                ) {
                    continue;
                }

                match redis.lpush(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "rpush" => {
                if parts.len() != 3 {
                    println!("Usage: RPUSH key value");
                    continue;
                }

                let command = format!("{} {}", parts[1], parts[2]);

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "rpush".to_string(),
                ) {
                    continue;
                }

                match redis.rpush(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "lpop" => {
                if parts.len() != 2 {
                    println!("Usage: LPOP key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lpop".to_string(),
                ) {
                    continue;
                }

                match redis.lpop(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "rpop" => {
                if parts.len() != 2 {
                    println!("Usage: RPOP key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "rpop".to_string(),
                ) {
                    continue;
                }

                match redis.rpop(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "llen" => {
                if parts.len() != 2 {
                    println!("Usage: LLEN key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "llen".to_string(),
                ) {
                    continue;
                }

                match redis.llen(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
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

                let command = format!("{} {}", parts[1], parts[2]);

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lindex".to_string(),
                ) {
                    continue;
                }

                match redis.lindex(
                    parts[1].to_string(),
                    index,
                ) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
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

                let command = format!(
                    "{} {} {}",
                    parts[1],
                    parts[2],
                    parts[3]
                );

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lset".to_string(),
                ) {
                    continue;
                }

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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "list".to_string(),
                    "lclear".to_string(),
                ) {
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hcreate".to_string(),
                ) {
                    continue;
                }

                match redis.hcreate(
                    parts[1].to_string(),
                    String::from(""),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hset" => {
                if parts.len() != 4 {
                    println!("Usage: HSET key field value");
                    continue;
                }

                let command = format!(
                    "{} {} {}",
                    parts[1],
                    parts[2],
                    parts[3]
                );

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hset".to_string(),
                ) {
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

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hget".to_string(),
                ) {
                    continue;
                }

                match redis.hget(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hexists" => {
                if parts.len() != 3 {
                    println!("Usage: HEXISTS key field");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hexists".to_string(),
                ) {
                    continue;
                }

                match redis.hexists(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hdel" => {
                if parts.len() != 3 {
                    println!("Usage: HDEL key field");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hdel".to_string(),
                ) {
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hlen".to_string(),
                ) {
                    continue;
                }

                match redis.hlen(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hclear" => {
                if parts.len() != 2 {
                    println!("Usage: HCLEAR key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hclear".to_string(),
                ) {
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hkeys".to_string(),
                ) {
                    continue;
                }

                match redis.hkeys(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "hvalues" => {
                if parts.len() != 2 {
                    println!("Usage: HVALUES key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "hash".to_string(),
                    "hvalues".to_string(),
                ) {
                    continue;
                }

                match redis.hvalues(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "screate".to_string(),
                ) {
                    continue;
                }

                match redis.screate(
                    parts[1].to_string(),
                    String::from(""),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "sadd" => {
                if parts.len() != 3 {
                    println!("Usage: SADD key value");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "sadd".to_string(),
                ) {
                    continue;
                }

                match redis.sadd(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "srem" => {
                if parts.len() != 3 {
                    println!("Usage: SREM key value");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "srem".to_string(),
                ) {
                    continue;
                }

                match redis.srem(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(()) => println!("OK"),
                    Err(e) => println!("(error) {}", e),
                }
            }

            "scontains" => {
                if parts.len() != 3 {
                    println!("Usage: SCONTAINS key value");
                    continue;
                }

                let command = format!(
                    "{} {}",
                    parts[1],
                    parts[2]
                );

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "scontains".to_string(),
                ) {
                    continue;
                }

                match redis.scontains(
                    parts[1].to_string(),
                    parts[2].to_string(),
                ) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "slen" => {
                if parts.len() != 2 {
                    println!("Usage: SLEN key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "slen".to_string(),
                ) {
                    continue;
                }

                match redis.slen(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
                    Err(e) => println!("(error) {}", e),
                }
            }

            "sclear" => {
                if parts.len() != 2 {
                    println!("Usage: SCLEAR key");
                    continue;
                }

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "sclear".to_string(),
                ) {
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

                let command = parts[1].to_string();

                if ModeHandler(
                    redis,
                    command,
                    "set".to_string(),
                    "svalues".to_string(),
                ) {
                    continue;
                }

                match redis.svalues(parts[1].to_string()) {
                    Ok(value) => {
                        println!("{}", String::from_utf8_lossy(&value));
                    }
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

fn ModeHandler(
    redis: &mut redis_client,
    command: String,
    t: String,
    operation: String,
) -> bool {
    // ========================================================
    // TRANSACTION MODE
    // ========================================================

    if redis.transaction_mode {
        let command = format!("{} {} {}", t, operation, command);

        redis.transaction_queue.push(command);

        println!("QUEUED");

        return true;
    }

    // ========================================================
    // SUBSCRIPTION MODE
    // Only SUBSCRIBE, UNSUBSCRIBE and PUBLISH are allowed
    // ========================================================

    if redis.subscription_mode {
        if operation != "subscribe"
            && operation != "unsubscribe"
            && operation != "publish"
        {
            println!(
                "(error) Can't execute '{}' while in subscription mode",
                operation
            );

            return true;
        }
    }

    false
}