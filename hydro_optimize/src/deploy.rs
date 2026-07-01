use std::collections::HashMap;
use std::sync::Arc;

use hydro_deploy::gcp::GcpNetwork;
use hydro_deploy::rust_crate::tracing_options::{
    AL2_PERF_SETUP_COMMAND, DEBIAN_PERF_SETUP_COMMAND, TracingOptions,
};
use hydro_deploy::{AwsNetwork, Deployment, Host};
use hydro_lang::deploy::TrybuildHost;

/// What the user provides when creating ReusableHosts
#[derive(PartialEq, Eq, Clone)]
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
    env: HashMap<String, String>,
    single_host: bool,
}

// Note: Aws AMIs vary by region. If you are changing the region, please also change the AMI.
const AWS_REGION: &str = "us-east-1";
const AWS_INSTANCE_AMI: &str = "ami-0521cb2d60cfbb1a6"; // Amazon Linux 2023
const AWS_INSTANCE_TYPE: &str = "m5.2xlarge"; // 8 vCPU, 32 GB RAM
const AWS_NUM_CORES: usize = 3; // Used for networking. Network cores will be pinned to these cores - 1. Empirically tested on m5.2xlarge.
/// m5.2xlarge: up to 10 Gbps network bandwidth
pub const AWS_NETWORK_BYTES_PER_SEC: f64 = 1_250_000_000.0;
/// m5.2xlarge: 12,000 baseline IOPS (EBS)
pub const AWS_IO_TPS: f64 = 12_000.0;
const GCP_REGION: &str = "us-central1-c";
const GCP_IMAGE: &str = "debian-cloud/debian-12";
const GCP_MACHINE_TYPE: &str = "n2-standard-4"; // 4 vCPU, 16 GB RAM
const GCP_NUM_CORES: usize = 4; // Used for pinning
const LOCAL_NUM_CORES: usize = 8; // Used for pinning, assuming the machine that launches the tests has 8 cores

impl ReusableHosts {
    pub fn new(host_type: &HostType) -> Self {
        let initialized = match host_type {
            HostType::Gcp { project } => InitializedHostType::Gcp {
                network: GcpNetwork::new(project.clone(), None),
                project: project.clone(),
            },
            HostType::Aws => InitializedHostType::Aws {
                network: AwsNetwork::new(AWS_REGION, None),
            },
            HostType::Localhost => InitializedHostType::Localhost,
        };

        Self {
            hosts: HashMap::new(),
            host_type: initialized,
            env: HashMap::new(),
            single_host: false,
        }
    }

    /// Add an environment variable that will be set for all hosts.
    /// If a value with the same key already exists, it will be overwritten.
    pub fn insert_env(&mut self, key: String, value: String) {
        self.env.insert(key, value);
    }

    /// When true, all locations share a single underlying host.
    pub fn set_single_host(&mut self, single_host: bool) {
        self.single_host = single_host;
    }

    pub fn num_cores(&self) -> usize {
        match &self.host_type {
            InitializedHostType::Gcp { .. } => GCP_NUM_CORES,
            InitializedHostType::Aws { .. } => AWS_NUM_CORES,
            InitializedHostType::Localhost => LOCAL_NUM_CORES,
        }
    }

    // NOTE: Creating hosts with the same display_name in the same deployment will result in undefined behavior.
    fn lazy_create_host(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
    ) -> Arc<dyn Host> {
        // All clusters share the same machine if single_host is true
        let key = if self.single_host {
            "shared".to_string()
        } else {
            display_name.clone()
        };
        self.hosts
            .entry(key)
            .or_insert_with(|| match &self.host_type {
                InitializedHostType::Gcp { project, network } => deployment
                    .GcpComputeEngineHost()
                    .project(project.clone())
                    .machine_type(GCP_MACHINE_TYPE)
                    .image(GCP_IMAGE)
                    .region(GCP_REGION)
                    .network(network.clone())
                    .display_name(display_name)
                    .add(),
                InitializedHostType::Aws { network } => deployment
                    .AwsEc2Host()
                    .region(AWS_REGION)
                    .instance_type(AWS_INSTANCE_TYPE)
                    .ami(AWS_INSTANCE_AMI)
                    .network(network.clone())
                    .display_name(display_name)
                    .add(),
                InitializedHostType::Localhost => deployment.Localhost(),
            })
            .clone()
    }

    fn get_rust_flags(&self) -> String {
        match &self.host_type {
            InitializedHostType::Gcp { .. } | InitializedHostType::Aws { .. } => {
                "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off -C link-arg=--no-rosegment".to_string()
            }
            InitializedHostType::Localhost => {
                "".to_string() // Compile fast! Localhost is used for debugging
            }
        }
    }

    fn host_with_env(&self, host: TrybuildHost) -> TrybuildHost {
        self.env.iter().fold(host, |host, (key, value)| {
            host.env(key.clone(), value.clone())
        })
    }

    pub fn get_process_host(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
        perf: bool,
    ) -> TrybuildHost {
        let mut host = TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .networking_cores(self.num_cores() - 1)
            .rustflags(self.get_rust_flags());
        if perf {
            let setup_command = match &self.host_type {
                InitializedHostType::Gcp { .. } => DEBIAN_PERF_SETUP_COMMAND.to_string(),
                InitializedHostType::Aws { .. } => AL2_PERF_SETUP_COMMAND.to_string(),
                InitializedHostType::Localhost => String::new(),
            };
            host = host.tracing(
                TracingOptions::builder()
                    .fold_outfile(format!("{}.data.folded", display_name))
                    .frequency(128)
                    .setup_command(setup_command)
                    .build(),
            );
        }
        self.host_with_env(host)
    }

    pub fn get_cluster_hosts(
        &mut self,
        deployment: &mut Deployment,
        cluster_name: String,
        num_hosts: usize,
        perf: bool,
    ) -> Vec<TrybuildHost> {
        (0..num_hosts)
            .map(|i| self.get_process_host(deployment, format!("{}{}", cluster_name, i), perf))
            .collect()
    }
}
