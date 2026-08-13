use std::{
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Barrier, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::{
    client::init::{self, redis_client},
    error::ServerError,
};

const COUNT: usize = 10_000;
const RUNS: usize = 5;

// ========================================================
// METRICS
// ========================================================

#[derive(Clone)]
struct BenchmarkMetrics {
    client_id: usize,
    operations: usize,
    time_ms: f64,
    throughput: f64,
    average_latency_us: f64,
    p50_latency_us: f64,
}

fn create_metrics(
    client_id: usize,
    operations: usize,
    elapsed: Duration,
    latencies: &[Duration],
) -> BenchmarkMetrics {
    let seconds = elapsed.as_secs_f64();

    let average_latency_us =
        (seconds / operations as f64) * 1_000_000.0;

    let p50_latency_us = calculate_p50(latencies);

    BenchmarkMetrics {
        client_id,
        operations,
        time_ms: seconds * 1000.0,
        throughput: operations as f64 / seconds,
        average_latency_us,
        p50_latency_us,
    }
}

// ========================================================
// P50
// ========================================================

fn calculate_p50(latencies: &[Duration]) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }

    let mut values: Vec<u128> =
        latencies.iter()
            .map(|d| d.as_nanos())
            .collect();

    values.sort_unstable();

    let middle = values.len() / 2;

    if values.len() % 2 == 0 {
        let value =
            (values[middle - 1] + values[middle]) / 2;

        value as f64 / 1000.0
    } else {
        values[middle] as f64 / 1000.0
    }
}

// ========================================================
// SINGLE CLIENT BENCHMARK
// ========================================================

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
// MULTI CLIENT BENCHMARK
// ========================================================

pub fn benchmark_multiple_clients(
    client_count: usize,
) {
    println!("==============================================================");
    println!("             Multi Client Redis Benchmark");
    println!("==============================================================");

    println!("Clients: {}", client_count);
    println!("Commands per test: {}", COUNT);
    println!("Each client runs each test ONCE");
    println!();

    // ========================================================
    // METRICS STORAGE
    // ========================================================

    let set_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::new()));

    let get_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::new()));

    let mixed_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::new()));

    // ========================================================
    // BARRIER
    // ========================================================

    let barrier =
        Arc::new(Barrier::new(client_count));

    let mut handles = Vec::new();

    // ========================================================
    // CREATE CLIENTS
    // ========================================================

    for client_id in 0..client_count {

        let barrier =
            Arc::clone(&barrier);

        let set_metrics =
            Arc::clone(&set_metrics);

        let get_metrics =
            Arc::clone(&get_metrics);

        let mixed_metrics =
            Arc::clone(&mixed_metrics);

        let handle =
            thread::spawn(move || {

                let client_number =
                    client_id + 1;

                println!(
                    "Client {}: Acquiring connection",
                    client_number
                );

                let result =
                    init::Init();

                match result {

                    Ok((mut redis)) => {

                        println!(
                            "Client {}: Connection acquired",
                            client_number
                        );

                        // ==================================================
                        // SET
                        // ==================================================

                        barrier.wait();

                        let mut latencies =
                            Vec::with_capacity(COUNT);

                        let start =
                            Instant::now();

                        for i in 0..COUNT {

                            let key = format!(
                                "client_{}_bench_key_{}",
                                client_id,
                                i
                            );

                            let value = format!(
                                "client_{}_bench_value_{}",
                                client_id,
                                i
                            );

                            let operation_start =
                                Instant::now();

                            if let Err(e) =
                                redis.set(key, value)
                            {
                                eprintln!(
                                    "Client {} SET failed: {}",
                                    client_number,
                                    e
                                );

                                return;
                            }

                            latencies.push(
                                operation_start.elapsed()
                            );
                        }

                        let elapsed =
                            start.elapsed();

                        let metric =
                            create_metrics(
                                client_number,
                                COUNT,
                                elapsed,
                                &latencies,
                            );

                        set_metrics
                            .lock()
                            .unwrap()
                            .push(metric);

                        // ==================================================
                        // GET
                        // ==================================================

                        barrier.wait();

                        let mut latencies =
                            Vec::with_capacity(COUNT);

                        let start =
                            Instant::now();

                        for i in 0..COUNT {

                            let key = format!(
                                "client_{}_bench_key_{}",
                                client_id,
                                i
                            );

                            let operation_start =
                                Instant::now();

                            if let Err(e) =
                                redis.get(key)
                            {
                                eprintln!(
                                    "Client {} GET failed: {}",
                                    client_number,
                                    e
                                );

                                return;
                            }

                            latencies.push(
                                operation_start.elapsed()
                            );
                        }

                        let elapsed =
                            start.elapsed();

                        let metric =
                            create_metrics(
                                client_number,
                                COUNT,
                                elapsed,
                                &latencies,
                            );

                        get_metrics
                            .lock()
                            .unwrap()
                            .push(metric);

                        // ==================================================
                        // SET + GET
                        // ==================================================

                        barrier.wait();

                        let mut latencies =
                            Vec::with_capacity(COUNT * 2);

                        let start =
                            Instant::now();

                        for i in 0..COUNT {

                            let key = format!(
                                "client_{}_mixed_key_{}",
                                client_id,
                                i
                            );

                            let value = format!(
                                "client_{}_mixed_value_{}",
                                client_id,
                                i
                            );

                            // --------------------------
                            // SET
                            // --------------------------

                            let operation_start =
                                Instant::now();

                            if let Err(e) =
                                redis.set(
                                    key.clone(),
                                    value,
                                )
                            {
                                eprintln!(
                                    "Client {} SET + GET failed: {}",
                                    client_number,
                                    e
                                );

                                return;
                            }

                            latencies.push(
                                operation_start.elapsed()
                            );

                            // --------------------------
                            // GET
                            // --------------------------

                            let operation_start =
                                Instant::now();

                            if let Err(e) =
                                redis.get(key)
                            {
                                eprintln!(
                                    "Client {} SET + GET failed: {}",
                                    client_number,
                                    e
                                );

                                return;
                            }

                            latencies.push(
                                operation_start.elapsed()
                            );
                        }

                        let elapsed =
                            start.elapsed();

                        let metric =
                            create_metrics(
                                client_number,
                                COUNT * 2,
                                elapsed,
                                &latencies,
                            );

                        mixed_metrics
                            .lock()
                            .unwrap()
                            .push(metric);

                        println!(
                            "Client {}: Benchmark completed",
                            client_number
                        );
                    }

                    Err(e) => {

                        eprintln!(
                            "Client {}: Failed to acquire connection: {}",
                            client_number,
                            e
                        );
                    }
                }
            });

        handles.push(handle);
    }

    // ========================================================
    // WAIT FOR ALL CLIENTS
    // ========================================================

    for handle in handles {

        if let Err(e) =
            handle.join()
        {
            eprintln!(
                "Client thread failed: {:?}",
                e
            );
        }
    }

    // ========================================================
    // GET METRICS
    // ========================================================

    let set_metrics =
        set_metrics.lock().unwrap().clone();

    let get_metrics =
        get_metrics.lock().unwrap().clone();

    let mixed_metrics =
        mixed_metrics.lock().unwrap().clone();

    // ========================================================
    // PRINT TABLES
    // ========================================================

    print_metrics_table(
        "SET",
        &set_metrics,
    );

    print_metrics_table(
        "GET",
        &get_metrics,
    );

    print_metrics_table(
        "SET + GET",
        &mixed_metrics,
    );

    // ========================================================
    // WRITE FILE
    // ========================================================

    write_metrics_to_file(
        client_count,
        &set_metrics,
        &get_metrics,
        &mixed_metrics,
    );

    println!();
    println!("==============================================================");
    println!("          Multi-client benchmark finished");
    println!("Results written to benchmark_results.txt");
    println!("==============================================================");
}

// ========================================================
// MULTI CLIENT SET
// ========================================================

fn benchmark_set_once(
    redis: &mut redis_client,
    client_id: usize,
) -> Result<(), ServerError> {

    for i in 0..COUNT {

        let key = format!(
            "client_{}_bench_key_{}",
            client_id,
            i
        );

        let value = format!(
            "client_{}_bench_value_{}",
            client_id,
            i
        );

        redis.set(key, value)?;
    }

    Ok(())
}

// ========================================================
// MULTI CLIENT GET
// ========================================================

fn benchmark_get_once(
    redis: &mut redis_client,
    client_id: usize,
) -> Result<(), ServerError> {

    for i in 0..COUNT {

        let key = format!(
            "client_{}_bench_key_{}",
            client_id,
            i
        );

        redis.get(key)?;
    }

    Ok(())
}

// ========================================================
// MULTI CLIENT SET + GET
// ========================================================

fn benchmark_set_get_once(
    redis: &mut redis_client,
    client_id: usize,
) -> Result<(), ServerError> {

    for i in 0..COUNT {

        let key = format!(
            "client_{}_mixed_key_{}",
            client_id,
            i
        );

        let value = format!(
            "client_{}_mixed_value_{}",
            client_id,
            i
        );

        redis.set(
            key.clone(),
            value,
        )?;

        redis.get(key)?;
    }

    Ok(())
}

// ========================================================
// PRINT MULTI CLIENT METRICS
// ========================================================

fn print_metrics_table(
    test_name: &str,
    metrics: &[BenchmarkMetrics],
) {
    println!();

    println!(
        "================================================================================"
    );

    println!(
        "                         {} RESULTS",
        test_name
    );

    println!(
        "================================================================================"
    );

    println!(
        "{:<10} {:>12} {:>14} {:>18} {:>18} {:>18}",
        "Client",
        "Operations",
        "Time (ms)",
        "Requests/sec",
        "Avg (us)",
        "p50 (us)"
    );

    println!(
        "{:-<10} {:-<12} {:-<14} {:-<18} {:-<18} {:-<18}",
        "",
        "",
        "",
        "",
        "",
        ""
    );

    let mut total_operations = 0usize;

    let mut max_time_ms = 0.0;

    let mut total_latency = 0.0;

    for metric in metrics {

        total_operations +=
            metric.operations;

        if metric.time_ms > max_time_ms {
            max_time_ms =
                metric.time_ms;
        }

        total_latency +=
            metric.average_latency_us;

        println!(
            "{:<10} {:>12} {:>14.3} {:>18.2} {:>18.3} {:>18.3}",
            format!("Client {}", metric.client_id),
            metric.operations,
            metric.time_ms,
            metric.throughput,
            metric.average_latency_us,
            metric.p50_latency_us
        );
    }

    // ========================================================
    // OVERALL CONCURRENT RESULT
    // ========================================================

    let total_seconds =
        max_time_ms / 1000.0;

    let overall_throughput =
        if total_seconds > 0.0 {
            total_operations as f64
                / total_seconds
        } else {
            0.0
        };

    let overall_average_latency =
        if total_operations > 0 {
            (total_seconds
                / total_operations as f64)
                * 1_000_000.0
        } else {
            0.0
        };

    let average_client_latency =
        if !metrics.is_empty() {
            total_latency
                / metrics.len() as f64
        } else {
            0.0
        };

    println!(
        "{:-<10} {:-<12} {:-<14} {:-<18} {:-<18} {:-<18}",
        "",
        "",
        "",
        "",
        "",
        ""
    );

    println!(
        "{:<10} {:>12} {:>14.3} {:>18.2} {:>18.3} {:>18.3}",
        "OVERALL",
        total_operations,
        max_time_ms,
        overall_throughput,
        average_client_latency,
        calculate_overall_p50(metrics)
    );

    println!(
        "================================================================================"
    );
}

// ========================================================
// OVERALL P50
// ========================================================

fn calculate_overall_p50(
    metrics: &[BenchmarkMetrics],
) -> f64 {

    if metrics.is_empty() {
        return 0.0;
    }

    let mut values =
        Vec::with_capacity(metrics.len());

    for metric in metrics {
        values.push(
            metric.p50_latency_us
        );
    }

    values.sort_by(|a, b|
        a.partial_cmp(b)
            .unwrap()
    );

    let middle =
        values.len() / 2;

    if values.len() % 2 == 0 {

        (values[middle - 1]
            + values[middle])
            / 2.0

    } else {

        values[middle]
    }
}

// ========================================================
// WRITE FILE
// ========================================================

fn write_metrics_to_file(
    client_count: usize,
    set_metrics: &[BenchmarkMetrics],
    get_metrics: &[BenchmarkMetrics],
    mixed_metrics: &[BenchmarkMetrics],
) {
    let mut file =
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("benchmark_results.txt")
        {
            Ok(file) => file,

            Err(e) => {
                eprintln!(
                    "Failed to create benchmark_results.txt: {}",
                    e
                );

                return;
            }
        };

    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(
        file,
        "                    MULTI CLIENT REDIS BENCHMARK"
    ).unwrap();

    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(
        file,
        "Clients: {}",
        client_count
    ).unwrap();

    writeln!(
        file,
        "Commands per test: {}",
        COUNT
    ).unwrap();

    writeln!(
        file,
        "Each client runs each test ONCE"
    ).unwrap();

    writeln!(file).unwrap();

    write_metrics_section(
        &mut file,
        "SET",
        set_metrics,
    );

    write_metrics_section(
        &mut file,
        "GET",
        get_metrics,
    );

    write_metrics_section(
        &mut file,
        "SET + GET",
        mixed_metrics,
    );

    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(
        file,
        "Benchmark finished successfully."
    ).unwrap();

    writeln!(
        file,
        "================================================================================"
    ).unwrap();
}

// ========================================================
// WRITE ONE SECTION
// ========================================================

fn write_metrics_section(
    file: &mut std::fs::File,
    test_name: &str,
    metrics: &[BenchmarkMetrics],
) {
    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(
        file,
        "                         {} RESULTS",
        test_name
    ).unwrap();

    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(
        file,
        "{:<10} {:>12} {:>14} {:>18} {:>18} {:>18}",
        "Client",
        "Operations",
        "Time (ms)",
        "Requests/sec",
        "Avg (us)",
        "p50 (us)"
    ).unwrap();

    writeln!(
        file,
        "{:-<10} {:-<12} {:-<14} {:-<18} {:-<18} {:-<18}",
        "",
        "",
        "",
        "",
        "",
        ""
    ).unwrap();

    let mut total_operations = 0usize;

    let mut max_time_ms = 0.0;

    let mut total_average_latency = 0.0;

    for metric in metrics {

        total_operations +=
            metric.operations;

        if metric.time_ms > max_time_ms {
            max_time_ms =
                metric.time_ms;
        }

        total_average_latency +=
            metric.average_latency_us;

        writeln!(
            file,
            "{:<10} {:>12} {:>14.3} {:>18.2} {:>18.3} {:>18.3}",
            format!("Client {}", metric.client_id),
            metric.operations,
            metric.time_ms,
            metric.throughput,
            metric.average_latency_us,
            metric.p50_latency_us
        ).unwrap();
    }

    // ========================================================
    // OVERALL
    // ========================================================

    let total_seconds =
        max_time_ms / 1000.0;

    let overall_throughput =
        if total_seconds > 0.0 {
            total_operations as f64
                / total_seconds
        } else {
            0.0
        };

    let overall_average_latency =
        if total_operations > 0 {
            (total_seconds
                / total_operations as f64)
                * 1_000_000.0
        } else {
            0.0
        };

    let average_client_latency =
        if !metrics.is_empty() {
            total_average_latency
                / metrics.len() as f64
        } else {
            0.0
        };

    let overall_p50 =
        calculate_overall_p50(metrics);

    writeln!(
        file,
        "{:-<10} {:-<12} {:-<14} {:-<18} {:-<18} {:-<18}",
        "",
        "",
        "",
        "",
        "",
        ""
    ).unwrap();

    writeln!(
        file,
        "{:<10} {:>12} {:>14.3} {:>18.2} {:>18.3} {:>18.3}",
        "OVERALL",
        total_operations,
        max_time_ms,
        overall_throughput,
        average_client_latency,
        overall_p50
    ).unwrap();

    writeln!(
        file,
        "================================================================================"
    ).unwrap();

    writeln!(file).unwrap();
}

// ========================================================
// SINGLE CLIENT SET
// ========================================================

fn benchmark_set(
    redis: &mut redis_client,
) -> Result<(), ServerError> {

    let mut total_time =
        Duration::ZERO;

    println!("======================================");
    println!("SET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {

        let start =
            Instant::now();

        for i in 0..COUNT {

            let key =
                format!("bench_key_{}", i);

            let value =
                format!("bench_value_{}", i);

            redis.set(key, value)?;
        }

        let elapsed =
            start.elapsed();

        total_time += elapsed;

        print_run_result(
            "SET",
            run,
            elapsed,
            COUNT,
        );
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
// SINGLE CLIENT GET
// ========================================================

fn benchmark_get(
    redis: &mut redis_client,
) -> Result<(), ServerError> {

    let mut total_time =
        Duration::ZERO;

    println!("======================================");
    println!("GET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {

        let start =
            Instant::now();

        for i in 0..COUNT {

            let key =
                format!("bench_key_{}", i);

            redis.get(key)?;
        }

        let elapsed =
            start.elapsed();

        total_time += elapsed;

        print_run_result(
            "GET",
            run,
            elapsed,
            COUNT,
        );
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
// SINGLE CLIENT SET + GET
// ========================================================

fn benchmark_set_get(
    redis: &mut redis_client,
) -> Result<(), ServerError> {

    let mut total_time =
        Duration::ZERO;

    println!("======================================");
    println!("SET + GET Benchmark");
    println!("======================================");

    for run in 1..=RUNS {

        let start =
            Instant::now();

        for i in 0..COUNT {

            let key =
                format!("mixed_key_{}", i);

            let value =
                format!("mixed_value_{}", i);

            redis.set(
                key.clone(),
                value,
            )?;

            redis.get(key)?;
        }

        let elapsed =
            start.elapsed();

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
// PRINT SINGLE CLIENT RUN
// ========================================================

fn print_run_result(
    name: &str,
    run: usize,
    elapsed: Duration,
    operations: usize,
) {
    let seconds =
        elapsed.as_secs_f64();

    let operations_f64 =
        operations as f64;

    println!(
        "--------------------------------------"
    );

    println!(
        "Test: {} | Run: {}",
        name,
        run
    );

    println!(
        "Operations: {}",
        operations
    );

    println!(
        "Time: {:.3} ms",
        seconds * 1000.0
    );

    println!(
        "Throughput: {:.2} ops/sec",
        operations_f64 / seconds
    );

    println!(
        "Average latency: {:.3} us",
        (seconds / operations_f64)
            * 1_000_000.0
    );

    println!(
        "--------------------------------------"
    );
}

// ========================================================
// PRINT SINGLE CLIENT FINAL
// ========================================================

fn print_final_result(
    name: &str,
    total_time: Duration,
    operations_per_run: usize,
    runs: usize,
) {
    let total_seconds =
        total_time.as_secs_f64();

    let average_seconds =
        total_seconds / runs as f64;

    let operations =
        operations_per_run as f64;

    let average_throughput =
        operations / average_seconds;

    let average_latency =
        (average_seconds / operations)
            * 1_000_000.0;

    println!();

    println!(
        "****************************************"
    );

    println!(
        "FINAL RESULT: {}",
        name
    );

    println!(
        "****************************************"
    );

    println!(
        "Runs: {}",
        runs
    );

    println!(
        "Operations per run: {}",
        operations_per_run
    );

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

    println!(
        "****************************************"
    );

    println!();
}