use std::collections::HashMap;
use std::sync::Arc;

use hydro_deploy::gcp::GcpNetwork;
use hydro_deploy::rust_crate::tracing_options::{
    AL2_PERF_SETUP_COMMAND, DEBIAN_PERF_SETUP_COMMAND, TracingOptions,
};
use hydro_deploy::{AwsNetwork, Deployment, Host, HostTargetType, LinuxCompileType};
use hydro_lang::deploy::TrybuildHost;

/// What the user provides when creating ReusableHosts
pub enum HostType {
    Gcp { project: String },
    Aws,
    Localhost,
}

/// Internal state with networks always initialized
enum InitializedHostType {
    Gcp {
        project: String,
        network: Arc<GcpNetwork>,
    },
    Aws {
        network: Arc<AwsNetwork>,
    },
    Localhost,
}

pub struct ReusableHosts {
    hosts: HashMap<String, Arc<dyn Host>>, // Key = display_name
    host_type: InitializedHostType,
}

// Note: Aws AMIs vary by region. If you are changing the region, please also change the AMI.
const AWS_REGION: &str = "us-west-2";
const AWS_INSTANCE_AMI: &str = "ami-055a9df0c8c9f681c"; // Amazon Linux 2

impl ReusableHosts {
    pub fn new(host_type: HostType) -> Self {
        let initialized = match host_type {
            HostType::Gcp { project } => InitializedHostType::Gcp {
                network: GcpNetwork::new(project.clone(), None),
                project,
            },
            HostType::Aws => InitializedHostType::Aws {
                network: AwsNetwork::new(AWS_REGION, None),
            },
            HostType::Localhost => InitializedHostType::Localhost,
        };

        Self {
            hosts: HashMap::new(),
            host_type: initialized,
        }
    }

    // NOTE: Creating hosts with the same display_name in the same deployment will result in undefined behavior.
    fn lazy_create_host(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> Arc<dyn Host> {
        self.hosts
            .entry(display_name.clone())
            .or_insert_with(|| match &self.host_type {
                InitializedHostType::Gcp { project, network } => deployment
                    .GcpComputeEngineHost()
                    .project(project.clone())
                    .machine_type("n2-standard-4")
                    .image("debian-cloud/debian-12")
                    .region("us-central1-c")
                    .network(network.clone())
                    .display_name(display_name)
                    // Better performance than MUSL, perf reporting fewer unidentified stacks, but requires launching from Linux
                    .target_type(HostTargetType::Linux(LinuxCompileType::Glibc))
                    .add(),
                InitializedHostType::Aws { network } => deployment
                    .AwsEc2Host()
                    .region(AWS_REGION)
                    .instance_type("t3.micro")
                    .ami(AWS_INSTANCE_AMI)
                    .network(network.clone())
                    // Better performance than MUSL, perf reporting fewer unidentified stacks, but requires launching from Linux
                    .target_type(HostTargetType::Linux(LinuxCompileType::Glibc))
                    .add(),
                InitializedHostType::Localhost => deployment.Localhost(),
            })
            .clone()
    }

    fn get_rust_flags(&self) -> String {
        match &self.host_type {
            InitializedHostType::Gcp { .. } | InitializedHostType::Aws { .. } => {
                "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off"
            }
            InitializedHostType::Localhost => {
                "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off"
            }
        }
        .to_string()
    }

    pub fn get_no_perf_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> TrybuildHost {
        TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .rustflags(self.get_rust_flags())
    }

    pub fn get_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> TrybuildHost {
        let setup_command = match &self.host_type {
            InitializedHostType::Gcp { .. } => DEBIAN_PERF_SETUP_COMMAND,
            InitializedHostType::Aws { .. } => AL2_PERF_SETUP_COMMAND,
            InitializedHostType::Localhost => "", // Isn't run on localhost anyway
        };
        TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .additional_hydro_features(vec!["runtime_measure".to_string()])
            .build_env(
                "HYDRO_RUNTIME_MEASURE_CPU_PREFIX",
                super::deploy_and_analyze::CPU_USAGE_PREFIX,
            )
            .rustflags(self.get_rust_flags())
            .tracing(
                TracingOptions::builder()
                    .perf_raw_outfile(format!("{}.perf.data", display_name.clone()))
                    .fold_outfile(format!("{}.data.folded", display_name))
                    .frequency(128)
                    .setup_command(setup_command)
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
            .map(|i| self.get_no_perf_process_hosts(deployment, format!("{}{}", cluster_name, i)))
            .collect()
    }
}
