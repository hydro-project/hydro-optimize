use std::collections::HashMap;
use std::sync::Arc;

use hydro_deploy::gcp::GcpNetwork;
use hydro_deploy::rust_crate::tracing_options::{DEBIAN_PERF_SETUP_COMMAND, TracingOptions};
use hydro_deploy::{Deployment, Host};
use hydro_lang::deploy::TrybuildHost;
use tokio::sync::RwLock;

pub struct ReusableHosts {
    pub hosts: HashMap<String, Arc<dyn Host>>, // Key = display_name
    pub host_arg: String,
    pub project: String,
    pub network: Arc<RwLock<GcpNetwork>>,
}

impl ReusableHosts {
    // NOTE: Creating hosts with the same display_name in the same deployment will result in undefined behavior.
    fn lazy_create_host(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> Arc<dyn Host> {
        self.hosts
            .entry(display_name.clone())
            .or_insert_with(|| {
                if self.host_arg == "gcp" {
                    deployment
                        .GcpComputeEngineHost()
                        .project(&self.project)
                        .machine_type("n2-standard-4")
                        .image("debian-cloud/debian-12")
                        .region("us-central1-c")
                        .network(self.network.clone())
                        .display_name(display_name)
                        .add()
                } else {
                    deployment.Localhost()
                }
            })
            .clone()
    }

    pub fn get_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> TrybuildHost {
        let rustflags = if self.host_arg == "gcp" {
            "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off -C link-args=--no-rosegment"
        } else {
            "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off"
        };
        TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .additional_hydro_features(vec!["runtime_measure".to_string()])
            .build_env(
                "HYDRO_RUNTIME_MEASURE_CPU_PREFIX",
                super::deploy_and_analyze::CPU_USAGE_PREFIX,
            )
            .rustflags(rustflags)
            .tracing(
                TracingOptions::builder()
                    .perf_raw_outfile(format!("{}.perf.data", display_name.clone()))
                    .fold_outfile(format!("{}.data.folded", display_name))
                    .frequency(128)
                    .setup_command(DEBIAN_PERF_SETUP_COMMAND)
                    .build(),
            )
    }

    pub fn get_cluster_hosts(
        &mut self,
        deployment: &mut Deployment,
        cluster_name: String,
        num_hosts: usize,
    ) -> Vec<TrybuildHost> {
        (0..num_hosts)
            .map(|i| self.get_process_hosts(deployment, format!("{}{}", cluster_name, i)))
            .collect()
    }
}
