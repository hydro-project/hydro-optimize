#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod lock_server;
pub mod network_calibrator;
pub mod simple_kv_bench;
// pub mod lobsters;
// pub mod web_submit;

use hydro_lang::prelude::Process;
use hydro_std::bench_client::BenchResult;
use stageleft::q;
use std::time::Duration;

/// Note: Must remain synchronized with definitions in hydro_optimize/deploy_and_analyze.
/// Redefined here because we don't want to import hydro_optimize as a dependency.
pub(crate) const LATENCY_PREFIX: &str = "HYDRO_OPTIMIZE_LAT:";
pub(crate) const THROUGHPUT_PREFIX: &str = "HYDRO_OPTIMIZE_THR:";

pub fn print_parseable_bench_results<'a, Aggregator>(
    aggregate_results: BenchResult<Process<'a, Aggregator>>,
) {
    aggregate_results.throughput.for_each(q!(move |throughput| {
        println!("{} {} requests/s", THROUGHPUT_PREFIX, throughput);
    }));
    aggregate_results
        .latency_histogram
        .for_each(q!(move |latencies| {
            let (p50, p99, p999, num_samples) = (
                Duration::from_nanos(latencies.borrow().value_at_quantile(0.5)).as_micros() as f64
                    / 1000.0,
                Duration::from_nanos(latencies.borrow().value_at_quantile(0.99)).as_micros() as f64
                    / 1000.0,
                Duration::from_nanos(latencies.borrow().value_at_quantile(0.999)).as_micros()
                    as f64
                    / 1000.0,
                latencies.borrow().len(),
            );
            println!(
                "{} p50: {:.3} | p99 {:.3} | p999 {:.3} ms ({:} samples)",
                LATENCY_PREFIX, p50, p99, p999, num_samples
            );
        }));
}
