# Apollo Puller

Executable program which pull [Ctrip Apollo](https://github.com/ctripcorp/apollo) configuration to local files, can be used as a sidecar.

## Usage

```shell
apollo-puller -c .config.yaml
```

Example config.yaml:

```yaml
# log_level: INFO
# worker_threads: 3
dir: "<dir of configurations>"
config_service_url: "<url of apollo config service>"
host:
  type: "HostName"  # HostName, HostCidr or Custom
apps:
- app_id: "<apollo app id>"
  namespaces:
  - application.properties
  - application.yaml
```

## License

MulanPSL-2.0.
