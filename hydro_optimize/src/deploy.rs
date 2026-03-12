use std::collections::HashMap;
use std::sync::Arc;

use hydro_deploy::gcp::GcpNetwork;
use hydro_deploy::rust_crate::tracing_options::{
    AL2_PERF_SETUP_COMMAND, DEBIAN_PERF_SETUP_COMMAND, TracingOptions,
};
use hydro_deploy::{AwsNetwork, Deployment, Host, HostTargetType, LinuxCompileType};
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
}

// Note: Aws AMIs vary by region. If you are changing the region, please also change the AMI.
const AWS_REGION: &str = "us-west-2";
const AWS_INSTANCE_AMI: &str = "ami-055a9df0c8c9f681c"; // Amazon Linux 2
const AWS_INSTANCE_TYPE: &str = "m5.2xlarge"; // 8 vCPU, 32 GB RAM
const AWS_NUM_CORES: usize = 8; // Used for pinning
const GCP_REGION: &str = "us-central1-c";
const GCP_IMAGE: &str = "debian-cloud/debian-12";
const GCP_MACHINE_TYPE: &str = "n2-standard-4"; // 4 vCPU, 16 GB RAM
const GCP_NUM_CORES: usize = 4; // Used for pinning

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
        }
    }

    /// Add an environment variable that will be set for all hosts.
    /// If a value with the same key already exists, it will be overwritten.
    pub fn insert_env(&mut self, key: String, value: String) {
        self.env.insert(key, value);
    }
   
    pub fn num_cores(&self) -> usize {
        match &self.host_type {
            InitializedHostType::Gcp { .. } => GCP_NUM_CORES,
            InitializedHostType::Aws { .. } => AWS_NUM_CORES,
            InitializedHostType::Localhost => 1, // Can't pin to cores locally anyway
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
                    .machine_type(GCP_MACHINE_TYPE)
                    .image(GCP_IMAGE)
                    .region(GCP_REGION)
                    .network(network.clone())
                    .display_name(display_name)
                    // Better performance than MUSL, perf reporting fewer unidentified stacks, but requires launching from Linux
                    .target_type(HostTargetType::Linux(LinuxCompileType::Glibc))
                    .add(),
                InitializedHostType::Aws { network } => deployment
                    .AwsEc2Host()
                    .region(AWS_REGION)
                    .instance_type(AWS_INSTANCE_TYPE)
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
                "" // Compile fast! Localhost is used for debugging
            }
        }
        .to_string()
    }

    fn host_with_env(&self, host: TrybuildHost) -> TrybuildHost {
        self.env.iter().fold(host, |host, (key, value)| {
            host.env(key.clone(), value.clone())
        })
    }

    pub fn get_no_perf_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
        pin_to_core: usize,
    ) -> TrybuildHost {
        let host = TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .pin_to_core(pin_to_core)
            .rustflags(self.get_rust_flags());
        self.host_with_env(host)
    }

    pub fn get_process_hosts(
        &mut self,
        deployment: &mut Deployment,
        display_name: String,
        pin_to_core: usize,
    ) -> TrybuildHost {
        let setup_command = match &self.host_type {
            InitializedHostType::Gcp { .. } => DEBIAN_PERF_SETUP_COMMAND,
            InitializedHostType::Aws { .. } => AL2_PERF_SETUP_COMMAND,
            InitializedHostType::Localhost => "", // Isn't run on localhost anyway
        };
        let host = TrybuildHost::new(self.lazy_create_host(deployment, display_name.clone()))
            .pin_to_core(pin_to_core)
            .rustflags(self.get_rust_flags())
            .tracing(
                TracingOptions::builder()
                    .perf_raw_outfile(format!("{}.perf.data", display_name.clone()))
                    .fold_outfile(format!("{}.data.folded", display_name))
                    .frequency(128)
                    .setup_command(setup_command)
                    .build(),
            );
        self.host_with_env(host)
    }

    pub fn get_cluster_hosts(
        &mut self,
        deployment: &mut Deployment,
        cluster_name: String,
        num_hosts: usize,
        pin_to_core: usize,
    ) -> Vec<TrybuildHost> {
        (0..num_hosts)
            .map(|i| self.get_no_perf_process_hosts(deployment, format!("{}{}", cluster_name, i), pin_to_core))
            .collect()
    }
}
