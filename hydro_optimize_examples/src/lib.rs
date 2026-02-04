#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod network_calibrator;
pub mod simple_kv_bench;
pub mod lock_server;
// pub mod lobsters;
// pub mod web_submit;

use std::time::Duration;
use hdrhistogram::Histogram;
use hydro_std::bench_client::{AggregateBenchResult, rolling_average::RollingAverage};
use stageleft::q;

/// Note: Must remain synchronized with definitions in hydro_optimize/deploy_and_analyze.
/// Redefined here because we don't want to import hydro_optimize as a dependency.
const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

pub fn print_parseable_bench_results<'a, Aggregator>(
    aggregate_results: AggregateBenchResult<'a, Aggregator>,
    interval_millis: u64,
) {
    aggregate_results
        .throughput
        .filter_map(q!(move |(throughputs, num_client_machines): (
            RollingAverage,
            usize
        )| {
            if let Some((lower, upper)) = throughputs.confidence_interval_99() {
                Some((
                    lower * num_client_machines as f64,
                    throughputs.sample_mean() * num_client_machines as f64,
                    upper * num_client_machines as f64,
                ))
            } else {
                None
            }
        }))
        .for_each(q!(move |(lower, mean, upper)| {
            println!(
                "{} {:.2} - {:.2} - {:.2} requests/s",
                THROUGHPUT_PREFIX, lower, mean, upper,
            );
        }));
    aggregate_results
        .latency
        .map(q!(move |latencies: Histogram<u64>| (
            Duration::from_nanos(latencies.value_at_quantile(0.5)).as_micros() as f64
                / interval_millis as f64,
            Duration::from_nanos(latencies.value_at_quantile(0.99)).as_micros() as f64
                / interval_millis as f64,
            Duration::from_nanos(latencies.value_at_quantile(0.999)).as_micros() as f64
                / interval_millis as f64,
            latencies.len(),
        )))
        .for_each(q!(move |(p50, p99, p999, num_samples)| {
            println!(
                "{} p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({:} samples)",
                LATENCY_PREFIX, p50, p99, p999, num_samples
            );
        }));
}