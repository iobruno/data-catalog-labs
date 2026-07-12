# IaC with Terraform

![Terraform](https://img.shields.io/badge/Terraform-1.15-black?style=flat&logo=terraform&logoColor=white&labelColor=573EDA)
![OpenTofu](https://img.shields.io/badge/OpenTofu-1.12-black?style=flat&logo=opentofu&logoColor=white&labelColor=573EDA)

[![Airbyte](https://img.shields.io/badge/Airbyte_Provider-1.0-007CEE?style=flat&logo=airbyte&logoColor=5F5DFF&labelColor=14193A)](https://registry.terraform.io/providers/airbytehq/airbyte/1.2.0)
[![Google](https://img.shields.io/badge/Google_Provider-7.39-3772FF?style=flat&logo=googlecloud&logoColor=white&labelColor=3772FF)](https://registry.terraform.io/providers/hashicorp/google/7.39.0)


## Getting started

Ensure you have either Terraform/OpenTofu installed to provision the resources faster - [tenv](https://github.com/tofuutils/tenv) recommended.

```shell
brew install tenv
```
```shell
tenv tf list-remote
tenv tf install 1.15.8
```
```shell
terraform --version
```
> Terraform v1.15.8
> on darwin_arm64


### IaC for BigQuery and Airbyte

1. Initialize your Terraform/OpenTofu env
```shell
terraform init
```

2. Create a .tfvars file, using [terraform.tfvars.example](./terraform.tfvars.example) as template:
```
cp terraform.tfvars.example terraform.tfvars
```

3. Edit the vars in terraform.tfvars accordingly and then run:
```shell
terraform plan
```
```shell
terraform apply -auto-approve
```


## TODO
- [x] Configure native Airbyte Connectors with Terraform/OpenTofu
- [x] Configure BigQuery dataset
