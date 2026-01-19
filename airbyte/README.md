# Airbyte OSS

![Python](https://img.shields.io/badge/Python-3.12-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
[![Airbyte](https://img.shields.io/badge/Airbyte-2.0.19-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
![Terraform](https://img.shields.io/badge/Terraform-1.14-black?style=flat&logo=terraform&logoColor=white&labelColor=573EDA)
![OpenTofu](https://img.shields.io/badge/OpenTofu-1.11-black?style=flat&logo=opentofu&logoColor=white&labelColor=573EDA)
[![CloudStorage](https://img.shields.io/badge/Google_Cloud-3772FF?style=flat&logo=googlecloud&logoColor=white&labelColor=3772FF)](https://console.cloud.google.com/)
[![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)](https://docs.docker.com/get-docker/)

Docker Compose support was [officially deprecated in August 2024 and is no longer supported](https://github.com/airbytehq/airbyte/discussions/40599).
Airbyte now uses abctl - a CLI tool that deploys Airbyte locally using [KinD](https://kind.sigs.k8s.io/).

Ensure you have either Terraform/OpenTofu installed to provision the resources faster - [tenv](https://github.com/tofuutils/tenv) recommended.


## Getting started

### Bootstraping Airbyte

1. Install abctl tool:
```shell
curl -LsfS https://get.airbyte.com | bash -
```

2. Execute:
```shell
abctl local install
```

3. Get your credentials with:
```shell
abctl local credentials
```

**4.** Airbyte WebUI can be accessed at:
```shell
open http://localhost:8000
```

**5.** Get the Workspace Id from the browser URL

- e.g.: `1a45e743-3de4-4652-adae-e5b2515668f5`.   
- from `http://localhost:8000/workspaces/1a45e743-3de4-4652-adae-e5b2515668f5/connections`


### IaC for BigQuery and Airbyte


1. Initialize your Terraform/OpenTofu env
```shell
terraform init
```

2. Create a .tfvars file, using [terraform.tfvars.example](../terraform/terraform.tfvars.example) as template and run:
```shell
terraform plan
```
```shell
terraform apply -auto-approve
```


## Custom Connectors

- T.B.D.


## TODO
- [x] Spin up Airbyte on Docker (KinD)
- [x] Configure native Airbyte Connectors with Terraform/OpenTofu
- [ ] Write a Custom Source connector for a REST API (HackerNews)
