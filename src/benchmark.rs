use std::time::{Duration, Instant};

use crate::{
    client::init::redis_client,
    error::ServerError,
};

const COUNT: usize = 10_000;
const RUNS: usize = 5;

pub fn benchmark(
    redis: &mut redis_client,
) -> Result<(), ServerError> {
    println!("======================================");
    println!("        Redis Clone Benchmark");
    println!("======================================");
    println!("Commands per test: {}", COUNT);
    println!("Benchmark runs: {}", RUNS);
    println!();

    benchmark_set(redis)?;
    benchmark_get(redis)?;
    benchmark_set_get(redis)?;

    println!("======================================");
    println!("      Benchmark completed successfully");
    println!("======================================");

    Ok(())
}

// ========================================================
// SET
// ========================================================

fn benchmark_set(
    redis: &mut redis_client,
) -> Result<(), ServerError> {
    let mut total_time = Duration::ZERO;

    println!("======================================");
    println!("SET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {
        let start = Instant::now();

        for i in 0..COUNT {
            let key = format!("bench_key_{}", i);
            let value = format!("bench_value_{}", i);

            redis.set(key, value)?;
        }

        let elapsed = start.elapsed();
        total_time += elapsed;

        print_run_result("SET", run, elapsed, COUNT);
    }

    print_final_result(
        "SET",
        total_time,
        COUNT,
        RUNS,
    );

    Ok(())
}

// ========================================================
// GET
// ========================================================

fn benchmark_get(
    redis: &mut redis_client,
) -> Result<(), ServerError> {
    let mut total_time = Duration::ZERO;

    println!("======================================");
    println!("GET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {
        let start = Instant::now();

        for i in 0..COUNT {
            let key = format!("bench_key_{}", i);

            redis.get(key)?;
        }

        let elapsed = start.elapsed();
        total_time += elapsed;

        print_run_result("GET", run, elapsed, COUNT);
    }

    print_final_result(
        "GET",
        total_time,
        COUNT,
        RUNS,
    );

    Ok(())
}

// ========================================================
// SET + GET
// ========================================================

fn benchmark_set_get(
    redis: &mut redis_client,
) -> Result<(), ServerError> {
    let mut total_time = Duration::ZERO;

    println!("======================================");
    println!("SET + GET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {
        let start = Instant::now();

        for i in 0..COUNT {
            let key = format!("mixed_key_{}", i);
            let value = format!("mixed_value_{}", i);

            redis.set(key.clone(), value)?;
            redis.get(key)?;
        }

        let elapsed = start.elapsed();
        total_time += elapsed;

        print_run_result(
            "SET + GET",
            run,
            elapsed,
            COUNT * 2,
        );
    }

    print_final_result(
        "SET + GET",
        total_time,
        COUNT * 2,
        RUNS,
    );

    Ok(())
}

// ========================================================
// PRINT INDIVIDUAL RUN
// ========================================================

fn print_run_result(
    name: &str,
    run: usize,
    elapsed: Duration,
    operations: usize,
) {
    let seconds = elapsed.as_secs_f64();
    let operations_f64 = operations as f64;

    println!("--------------------------------------");
    println!("Test: {} | Run: {}", name, run);
    println!("Operations: {}", operations);
    println!("Time: {:.3} ms", seconds * 1000.0);

    println!(
        "Throughput: {:.2} ops/sec",
        operations_f64 / seconds
    );

    println!(
        "Average latency: {:.3} us",
        (seconds / operations_f64) * 1_000_000.0
    );

    println!("--------------------------------------");
}

// ========================================================
// PRINT FINAL AVERAGE
// ========================================================

fn print_final_result(
    name: &str,
    total_time: Duration,
    operations_per_run: usize,
    runs: usize,
) {
    let total_seconds = total_time.as_secs_f64();

    let average_seconds =
        total_seconds / runs as f64;

    let operations = operations_per_run as f64;

    let average_throughput =
        operations / average_seconds;

    let average_latency =
        (average_seconds / operations)
        * 1_000_000.0;

    println!();
    println!("****************************************");
    println!("FINAL RESULT: {}", name);
    println!("****************************************");

    println!("Runs: {}", runs);
    println!("Operations per run: {}", operations_per_run);

    println!(
        "Average time: {:.3} ms",
        average_seconds * 1000.0
    );

    println!(
        "Average throughput: {:.2} ops/sec",
        average_throughput
    );

    println!(
        "Average latency: {:.3} us",
        average_latency
    );

    println!("****************************************");
    println!();
}