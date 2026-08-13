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

// ========================================================
// RUN RESULT
// ========================================================

#[derive(Clone)]
struct RunResult {
    set_metrics: Vec<BenchmarkMetrics>,
    get_metrics: Vec<BenchmarkMetrics>,
    mixed_metrics: Vec<BenchmarkMetrics>,
}

// ========================================================
// CREATE METRICS
// ========================================================

fn create_metrics(
    client_id: usize,
    operations: usize,
    elapsed: Duration,
    latencies: &[Duration],
) -> BenchmarkMetrics {
    let seconds = elapsed.as_secs_f64();

    let average_latency_us =
        if operations > 0 {
            (seconds / operations as f64) * 1_000_000.0
        } else {
            0.0
        };

    let p50_latency_us = calculate_p50(latencies);

    BenchmarkMetrics {
        client_id,
        operations,
        time_ms: seconds * 1000.0,
        throughput: if seconds > 0.0 {
            operations as f64 / seconds
        } else {
            0.0
        },
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
    runs: usize,
) {
    println!("==============================================================");
    println!("             Multi Client Redis Benchmark");
    println!("==============================================================");

    println!("Clients: {}", client_count);
    println!("Commands per test: {}", COUNT);
    println!("Benchmark runs: {}", runs);
    println!("Each client runs each test once per run");
    println!();

    if client_count == 0 {
        println!("Client count must be greater than zero.");
        return;
    }

    if runs == 0 {
        println!("Number of runs must be greater than zero.");
        return;
    }

    // ========================================================
    // STORE ALL RUN RESULTS
    // ========================================================

    let mut all_runs: Vec<RunResult> =
        Vec::with_capacity(runs);

    // ========================================================
    // RUN BENCHMARK
    // ========================================================

    for run in 1..=runs {
        println!();
        println!("##############################################################");
        println!("# RUN {}", run);
        println!("##############################################################");
        println!();

        let result =
            run_multi_client_benchmark(client_count);

        match result {
            Some(run_result) => {
                print_metrics_table(
                    "SET",
                    &run_result.set_metrics,
                );

                print_metrics_table(
                    "GET",
                    &run_result.get_metrics,
                );

                print_metrics_table(
                    "SET + GET",
                    &run_result.mixed_metrics,
                );

                all_runs.push(run_result);
            }

            None => {
                eprintln!(
                    "Run {} failed. Stopping benchmark.",
                    run
                );

                break;
            }
        }
    }

    if all_runs.is_empty() {
        println!("No successful benchmark runs.");
        return;
    }

    // ========================================================
    // FINAL COLLECTIVE RESULTS
    // ========================================================

    print_collective_result(
        "SET",
        &all_runs,
        |run| &run.set_metrics,
    );

    print_collective_result(
        "GET",
        &all_runs,
        |run| &run.get_metrics,
    );

    print_collective_result(
        "SET + GET",
        &all_runs,
        |run| &run.mixed_metrics,
    );

    // ========================================================
    // WRITE FILE
    // ========================================================

    write_metrics_to_file(
        client_count,
        runs,
        &all_runs,
    );

    println!();
    println!("################################################################");
    println!("# Benchmark finished");
    println!();
    println!("# Results written to benchmark_results.txt");
    println!("################################################################");
}

// ========================================================
// RUN ONE MULTI CLIENT BENCHMARK
// ========================================================

fn run_multi_client_benchmark(
    client_count: usize,
) -> Option<RunResult> {

    let set_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::with_capacity(client_count)));

    let get_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::with_capacity(client_count)));

    let mixed_metrics:
        Arc<Mutex<Vec<BenchmarkMetrics>>> =
        Arc::new(Mutex::new(Vec::with_capacity(client_count)));

    // ========================================================
    // BARRIER
    // ========================================================

    let barrier =
        Arc::new(Barrier::new(client_count));

    let mut handles = Vec::with_capacity(client_count);

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
            thread::spawn(move || -> bool {

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

                            let key =
                                format!(
                                    "client_{}_bench_key_{}",
                                    client_id,
                                    i
                                );

                            let value =
                                format!(
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

                                return false;
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

                            let key =
                                format!(
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

                                return false;
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

                            let key =
                                format!(
                                    "client_{}_mixed_key_{}",
                                    client_id,
                                    i
                                );

                            let value =
                                format!(
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

                                return false;
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

                                return false;
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

                        true
                    }

                    Err(e) => {

                        eprintln!(
                            "Client {}: Failed to acquire connection: {}",
                            client_number,
                            e
                        );

                        false
                    }
                }
            });

        handles.push(handle);
    }

    // ========================================================
    // WAIT FOR THREADS
    // ========================================================

    let mut success = true;

    for handle in handles {

        match handle.join() {

            Ok(result) => {
                if !result {
                    success = false;
                }
            }

            Err(e) => {
                eprintln!(
                    "Client thread failed: {:?}",
                    e
                );

                success = false;
            }
        }
    }

    if !success {
        return None;
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

    // Make sure every client produced metrics.
    if set_metrics.len() != client_count
        || get_metrics.len() != client_count
        || mixed_metrics.len() != client_count
    {
        eprintln!(
            "Not all clients produced benchmark metrics."
        );

        return None;
    }

    Some(RunResult {
        set_metrics,
        get_metrics,
        mixed_metrics,
    })
}

// ========================================================
// PRINT METRICS TABLE
// ========================================================

fn print_metrics_table(
    test_name: &str,
    metrics: &[BenchmarkMetrics],
) {
    println!();

    println!(
        "Client       Operations      Time (ms)       Requests/sec           Avg (us)           p50 (us)"
    );

    println!();

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

        println!(
            "Client {:<5} {:>12} {:>14.3} {:>20.2} {:>18.3} {:>18.3}",
            metric.client_id,
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

    // This is the average latency across clients.
    // Do NOT divide wall-clock time by all concurrent operations.
    let overall_average_latency =
        if !metrics.is_empty() {
            total_average_latency
                / metrics.len() as f64
        } else {
            0.0
        };

    let overall_p50 =
        calculate_overall_p50(metrics);

    println!();

    println!(
        "# OVERALL {:>14} {:>14.3} {:>20.2} {:>18.3} {:>18.3}",
        total_operations,
        max_time_ms,
        overall_throughput,
        overall_average_latency,
        overall_p50
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

        (
            values[middle - 1]
                + values[middle]
        ) / 2.0

    } else {

        values[middle]
    }
}

// ========================================================
// FINAL COLLECTIVE RESULT
// ========================================================

fn print_collective_result<F>(
    test_name: &str,
    runs: &[RunResult],
    get_metrics: F,
)
where
    F: Fn(&RunResult) -> &Vec<BenchmarkMetrics>,
{
    println!();
    println!("################################################################");
    println!("# FINAL COLLECTIVE RESULT: {}", test_name);
    println!("################################################################");
    println!();

    if runs.is_empty() {
        println!("No successful runs.");
        return;
    }

    let mut total_operations = 0usize;

    let mut total_time_ms = 0.0;

    let mut total_throughput = 0.0;

    let mut total_average_latency = 0.0;

    let mut all_p50_values = Vec::new();

    for run in runs {

        let metrics =
            get_metrics(run);

        if metrics.is_empty() {
            continue;
        }

        let mut run_operations = 0usize;

        let mut run_max_time_ms = 0.0;

        let mut run_average_latency = 0.0;

        for metric in metrics {

            run_operations +=
                metric.operations;

            if metric.time_ms > run_max_time_ms {
                run_max_time_ms =
                    metric.time_ms;
            }

            run_average_latency +=
                metric.average_latency_us;

            all_p50_values.push(
                metric.p50_latency_us
            );
        }

        run_average_latency /=
            metrics.len() as f64;

        let run_seconds =
            run_max_time_ms / 1000.0;

        let run_throughput =
            if run_seconds > 0.0 {
                run_operations as f64
                    / run_seconds
            } else {
                0.0
            };

        total_operations +=
            run_operations;

        total_time_ms +=
            run_max_time_ms;

        total_throughput +=
            run_throughput;

        total_average_latency +=
            run_average_latency;
    }

    let successful_runs =
        runs.len() as f64;

    // ========================================================
    // AVERAGES ACROSS RUNS
    // ========================================================

    let average_operations =
        total_operations as f64
            / successful_runs;

    let average_time_ms =
        total_time_ms
            / successful_runs;

    let average_throughput =
        total_throughput
            / successful_runs;

    let average_latency =
        total_average_latency
            / successful_runs;

    // ========================================================
    // COLLECTIVE P50
    // ========================================================

    all_p50_values.sort_by(|a, b|
        a.partial_cmp(b)
            .unwrap()
    );

    let collective_p50 =
        if all_p50_values.is_empty() {

            0.0

        } else {

            let middle =
                all_p50_values.len() / 2;

            if all_p50_values.len() % 2 == 0 {

                (
                    all_p50_values[middle - 1]
                        + all_p50_values[middle]
                ) / 2.0

            } else {

                all_p50_values[middle]
            }
        };

    // ========================================================
    // PRINT
    // ========================================================

    println!(
        "Runs:                    {}",
        runs.len()
    );

    println!(
        "Operations per run:     {:.0}",
        average_operations
    );

    println!(
        "Total operations:       {}",
        total_operations
    );

    println!(
        "Average time:            {:.3} ms",
        average_time_ms
    );

    println!(
        "Average throughput:      {:.2} ops/sec",
        average_throughput
    );

    println!(
        "Average latency:         {:.3} us",
        average_latency
    );

    println!(
        "Collective p50:          {:.3} us",
        collective_p50
    );

    println!();
}

// ========================================================
// WRITE ALL RESULTS TO FILE
// ========================================================

fn write_metrics_to_file(
    client_count: usize,
    runs_requested: usize,
    all_runs: &[RunResult],
) {
    let mut file =
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("benchmark_results.md")
        {
            Ok(file) => file,

            Err(e) => {
                eprintln!(
                    "Failed to create benchmark_results.md: {}",
                    e
                );

                return;
            }
        };

    writeln!(
        file,
        "################################################################"
    ).unwrap();

    writeln!(
        file,
        "# MULTI CLIENT REDIS BENCHMARK"
    ).unwrap();

    writeln!(
        file,
        "################################################################"
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
        "Benchmark runs: {}",
        runs_requested
    ).unwrap();

    writeln!(
        file,
        "Successful runs: {}",
        all_runs.len()
    ).unwrap();

    writeln!(file).unwrap();

    // ========================================================
    // WRITE EACH RUN
    // ========================================================

    for (index, run) in all_runs.iter().enumerate() {

        writeln!(
            file,
            "################################################################"
        ).unwrap();

        writeln!(
            file,
            "# RUN {}",
            index + 1
        ).unwrap();

        writeln!(
            file,
            "################################################################"
        ).unwrap();

        writeln!(file).unwrap();

        write_metrics_section(
            &mut file,
            "SET",
            &run.set_metrics,
        );

        write_metrics_section(
            &mut file,
            "GET",
            &run.get_metrics,
        );

        write_metrics_section(
            &mut file,
            "SET + GET",
            &run.mixed_metrics,
        );
    }

    // ========================================================
    // FINAL COLLECTIVE RESULTS
    // ========================================================

    write_collective_result(
        &mut file,
        "SET",
        all_runs,
        |run| &run.set_metrics,
    );

    write_collective_result(
        &mut file,
        "GET",
        all_runs,
        |run| &run.get_metrics,
    );

    write_collective_result(
        &mut file,
        "SET + GET",
        all_runs,
        |run| &run.mixed_metrics,
    );

    writeln!(
        file,
        "################################################################"
    ).unwrap();

    writeln!(
        file,
        "# Benchmark finished"
    ).unwrap();

    writeln!(
        file,
        "################################################################"
    ).unwrap();
}

// ========================================================
// WRITE ONE RUN SECTION
// ========================================================

fn write_metrics_section(
    file: &mut std::fs::File,
    test_name: &str,
    metrics: &[BenchmarkMetrics],
) {
    writeln!(
        file,
        "Client       Operations      Time (ms)       Requests/sec           Avg (us)           p50 (us)"
    ).unwrap();

    writeln!(file).unwrap();

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
            "Client {:<5} {:>12} {:>14.3} {:>20.2} {:>18.3} {:>18.3}",
            metric.client_id,
            metric.operations,
            metric.time_ms,
            metric.throughput,
            metric.average_latency_us,
            metric.p50_latency_us
        ).unwrap();
    }

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
        if !metrics.is_empty() {
            total_average_latency
                / metrics.len() as f64
        } else {
            0.0
        };

    let overall_p50 =
        calculate_overall_p50(metrics);

    writeln!(file).unwrap();

    writeln!(
        file,
        "# OVERALL {:>14} {:>14.3} {:>20.2} {:>18.3} {:>18.3}",
        total_operations,
        max_time_ms,
        overall_throughput,
        overall_average_latency,
        overall_p50
    ).unwrap();

    writeln!(file).unwrap();
}

// ========================================================
// WRITE COLLECTIVE RESULT
// ========================================================

fn write_collective_result<F>(
    file: &mut std::fs::File,
    test_name: &str,
    runs: &[RunResult],
    get_metrics: F,
)
where
    F: Fn(&RunResult) -> &Vec<BenchmarkMetrics>,
{
    writeln!(file).unwrap();

    writeln!(
        file,
        "################################################################"
    ).unwrap();

    writeln!(
        file,
        "# FINAL COLLECTIVE RESULT: {}",
        test_name
    ).unwrap();

    writeln!(
        file,
        "################################################################"
    ).unwrap();

    if runs.is_empty() {
        writeln!(
            file,
            "No successful runs."
        ).unwrap();

        return;
    }

    let mut total_operations = 0usize;

    let mut total_time_ms = 0.0;

    let mut total_throughput = 0.0;

    let mut total_average_latency = 0.0;

    let mut all_p50_values = Vec::new();

    for run in runs {

        let metrics =
            get_metrics(run);

        if metrics.is_empty() {
            continue;
        }

        let mut run_operations = 0usize;

        let mut run_max_time_ms = 0.0;

        let mut run_average_latency = 0.0;

        for metric in metrics {

            run_operations +=
                metric.operations;

            if metric.time_ms > run_max_time_ms {
                run_max_time_ms =
                    metric.time_ms;
            }

            run_average_latency +=
                metric.average_latency_us;

            all_p50_values.push(
                metric.p50_latency_us
            );
        }

        run_average_latency /=
            metrics.len() as f64;

        let run_seconds =
            run_max_time_ms / 1000.0;

        let run_throughput =
            if run_seconds > 0.0 {
                run_operations as f64
                    / run_seconds
            } else {
                0.0
            };

        total_operations +=
            run_operations;

        total_time_ms +=
            run_max_time_ms;

        total_throughput +=
            run_throughput;

        total_average_latency +=
            run_average_latency;
    }

    let run_count =
        runs.len() as f64;

    let average_operations =
        total_operations as f64
            / run_count;

    let average_time_ms =
        total_time_ms
            / run_count;

    let average_throughput =
        total_throughput
            / run_count;

    let average_latency =
        total_average_latency
            / run_count;

    all_p50_values.sort_by(|a, b|
        a.partial_cmp(b)
            .unwrap()
    );

    let collective_p50 =
        if all_p50_values.is_empty() {

            0.0

        } else {

            let middle =
                all_p50_values.len() / 2;

            if all_p50_values.len() % 2 == 0 {

                (
                    all_p50_values[middle - 1]
                        + all_p50_values[middle]
                ) / 2.0

            } else {

                all_p50_values[middle]
            }
        };

    writeln!(
        file,
        "Runs:                    {}",
        runs.len()
    ).unwrap();

    writeln!(
        file,
        "Operations per run:      {:.0}",
        average_operations
    ).unwrap();

    writeln!(
        file,
        "Total operations:        {}",
        total_operations
    ).unwrap();

    writeln!(
        file,
        "Average time:            {:.3} ms",
        average_time_ms
    ).unwrap();

    writeln!(
        file,
        "Average throughput:      {:.2} ops/sec",
        average_throughput
    ).unwrap();

    writeln!(
        file,
        "Average latency:         {:.3} us",
        average_latency
    ).unwrap();

    writeln!(
        file,
        "Collective p50:          {:.3} us",
        collective_p50
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

    for i in 0..COUNT {

        let key =
            format!("bench_key_{}", i);

        let value =
            format!("bench_value_{}", i);

        let start =
            Instant::now();

        redis.set(key, value)?;

        total_time +=
            start.elapsed();
    }

    println!(
        "Total time: {:.3} ms",
        total_time.as_secs_f64() * 1000.0
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

    for i in 0..COUNT {

        let key =
            format!("bench_key_{}", i);

        let start =
            Instant::now();

        redis.get(key)?;

        total_time +=
            start.elapsed();
    }

    println!(
        "Total time: {:.3} ms",
        total_time.as_secs_f64() * 1000.0
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

    for i in 0..COUNT {

        let key =
            format!("mixed_key_{}", i);

        let value =
            format!("mixed_value_{}", i);

        let start =
            Instant::now();

        redis.set(
            key.clone(),
            value,
        )?;

        redis.get(key)?;

        total_time +=
            start.elapsed();
    }

    println!(
        "Total time: {:.3} ms",
        total_time.as_secs_f64() * 1000.0
    );

    Ok(())
}