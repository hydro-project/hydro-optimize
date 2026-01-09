use std::collections::HashMap;
use std::sync::Arc;

use hydro_deploy::gcp::GcpNetwork;
use hydro_deploy::rust_crate::tracing_options::{
    AL2_PERF_SETUP_COMMAND, DEBIAN_PERF_SETUP_COMMAND, TracingOptions,
};
use hydro_deploy::{AwsNetwork, Deployment, Host};
use hydro_lang::deploy::TrybuildHost;

/// What the user provides when creating ReusableHosts
pub enum HostType {
    GCP { project: String },
    AWS,
    Localhost,
}

/// Internal state with networks always initialized
enum InitializedHostType {
    GCP {
        project: String,
        network: Arc<GcpNetwork>,
    },
    AWS {
        network: Arc<AwsNetwork>,
    },
    Localhost,
}

pub struct ReusableHosts {
    hosts: HashMap<String, Arc<dyn Host>>, // Key = display_name
    host_type: InitializedHostType,
}

// Note: AWS AMIs vary by region. If you are changing the region, please also change the AMI.
const AWS_REGION: &str = "us-east-1";
const AWS_INSTANCE_AMI: &str = "ami-0e95a5e2743ec9ec9";

impl ReusableHosts {
    pub fn new(host_type: HostType) -> Self {
        let initialized = match host_type {
            HostType::GCP { project } => InitializedHostType::GCP {
                network: GcpNetwork::new(project.clone(), None),
                project,
            },
            HostType::AWS => InitializedHostType::AWS {
                network: AwsNetwork::new("us-east-1", None),
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
                InitializedHostType::GCP { project, network } => deployment
                    .GcpComputeEngineHost()
                    .project(project.clone())
                    .machine_type("n2-standard-4")
                    .image("debian-cloud/debian-12")
                    .region("us-central1-c")
                    .network(network.clone())
                    .display_name(display_name)
                    .add(),
                InitializedHostType::AWS { network } => deployment
                    .AwsEc2Host()
                    .region(AWS_REGION)
                    .instance_type("t3.micro")
                    .ami(AWS_INSTANCE_AMI) // Amazon Linux 2
                    .network(network.clone())
                    .add(),
                InitializedHostType::Localhost => deployment.Localhost(),
            })
            .clone()
    }

    pub fn get_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> TrybuildHost {
        let rustflags = match &self.host_type {
            InitializedHostType::GCP { .. } | InitializedHostType::AWS { .. } => {
                "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off -C link-args=--no-rosegment"
            }
            InitializedHostType::Localhost => {
                "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off"
            }
        };
        let setup_command = match &self.host_type {
            InitializedHostType::GCP { .. } => DEBIAN_PERF_SETUP_COMMAND,
            InitializedHostType::AWS { .. } => AL2_PERF_SETUP_COMMAND,
            InitializedHostType::Localhost => "", // Isn't run on localhost anyway
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
            .map(|i| self.get_process_hosts(deployment, format!("{}{}", cluster_name, i)))
            .collect()
    }
}
