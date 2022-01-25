// Copyright (c) 2021 jmjoy.
//
// Apollo Puller is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

use apollo_client::{conf::{
    meta::IpValue, requests::WatchRequest, ApolloConfClient, ApolloConfClientBuilder,
}, utils::canonicalize_namespace};
use cidr_utils::cidr::IpCidr;
use clap::Parser;
use futures_util::{future::join_all, pin_mut, stream::StreamExt};
use ini::Ini;
use log::LevelFilter;
use log4rs::{append::console::ConsoleAppender, config::Appender};
use serde::Deserialize;
use std::{path::{PathBuf, Path}, sync::Arc};
use tokio::{fs::{self, File}, runtime, io::AsyncWriteExt};
use url::Url;

/// Command line arguments.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    config: PathBuf,
}

/// Config file format.
#[derive(Deserialize)]
struct Config {
    /// Log level, choose OFF, ERROR, WARN, INFO, DEBUG or TRACE.
    #[serde(default = "default_log_level")]
    log_level: LevelFilter,

    /// Worker threads of async runtime.
    worker_threads: Option<usize>,

    /// Directory of generated configuration files.
    dir: PathBuf,

    /// Config service url of apollo.
    config_service_url: String,

    /// Host identity.
    host: Option<Host>,

    /// Apollo apps.
    apps: Vec<App>,
}

fn default_log_level() -> LevelFilter {
    LevelFilter::Info
}

/// Field of config file format.
#[derive(Deserialize)]
struct App {
    /// App id of apollo config app.
    app_id: String,

    /// Namespaces of apollo config app ().
    namespaces: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum Host {
    HostName,
    HostCidr { cidr: String },
    Custom { custom: String },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config_file = std::fs::File::open(&args.config)?;
    let config: Config = serde_yaml::from_reader(config_file)?;
    init_log(&config)?;

    let mut rt_builder = runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if let Some(worker_threads) = config.worker_threads {
        rt_builder.worker_threads(worker_threads);
    }
    let rt = rt_builder.build()?;

    rt.block_on(run(config))?;

    Ok(())
}

fn init_log(config: &Config) -> anyhow::Result<()> {
    let stdout = ConsoleAppender::builder().build();

    log4rs::init_config(
        log4rs::config::Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout)))
            .build(
                log4rs::config::Root::builder()
                    .appender("stdout")
                    .build(config.log_level.clone()),
            )?,
    )?;

    Ok(())
}

async fn run(config: Config) -> anyhow::Result<()> {
    fs::create_dir_all(&config.dir).await?;

    // Create configuration client.
    let client =
        ApolloConfClientBuilder::new_via_config_service(Url::parse(&config.config_service_url)?)?
            .build()?;

    let client = Arc::new(client);

    let ip_value = config.host.as_ref().map(host_to_ip_value).transpose()?;

    let futs = config.apps.iter().map(|app| {
        let client = client.clone();
        let ip_value = ip_value.clone();
        let base_dir = config.dir.clone();

        Box::pin(async move {
            run_app(&client, ip_value, app, &base_dir).await;
        })
    });

    join_all(futs).await;

    Ok(())
}

async fn run_app(client: &ApolloConfClient, ip_value: Option<IpValue>, app: &App, base_dir: &Path) {
    let stream = client.watch(WatchRequest {
        app_id: app.app_id.clone(),
        namespace_names: app.namespaces.clone(),
        ip: ip_value.clone(),
        ..Default::default()
    });

    pin_mut!(stream);

    while let Some(responses) = stream.next().await {
        let f = async {
            let responses = responses?;

            for (_, response) in responses {
                let response = response?;

                let mut path = base_dir.to_path_buf();
                path.push(response.app_id);
                fs::create_dir_all(&path).await?;

                let filename = canonicalize_namespace(&response.namespace_name);
                let content = if filename.ends_with(".properties") {
                    let mut content = Vec::new();
                    let mut conf = Ini::new();
                    for (key, value) in response.configurations {
                        conf.with_section(None::<&str>).set(key, value);
                    }
                    conf.write_to(&mut content)?;
                    content
                } else {
                    let content = response.configurations.get("content").map(|s| s.as_str()).unwrap_or_default();
                    content.as_bytes().to_vec()
                };

                path.push(filename);
                let mut file = File::create(path).await?;
                file.write_all(&content).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        if let Err(e) = f.await {
            log::error!("{:?}", e);
            continue;
        }
    }
}

fn host_to_ip_value(host: &Host) -> anyhow::Result<IpValue> {
    match host {
        Host::HostName => Ok(IpValue::HostName),
        Host::HostCidr { cidr } => Ok(IpValue::HostCidr(IpCidr::from_str(cidr)?)),
        Host::Custom { custom } => Ok(IpValue::Custom(custom.clone())),
    }
}
