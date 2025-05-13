INFRA_INIT_TF_BUCKET_NAME=muz-designed-msc-data-ai-2025-infra-init-tfstate
SECRET_NAME = GCP_SA_KEY

tf-init-infra-init:
	@cd infra-init && terraform init

tf-plan-infra-init:
	@cd infra-init && terraform plan

tf-apply-infra-init:
	@cd infra-init && terraform apply

create-backend-infra-init:
	echo 'terraform {' > infra-init/backend.tf
	echo '  backend "gcs" {' >> infra-init/backend.tf
	echo '    bucket = "$(INFRA_INIT_TF_BUCKET_NAME)"' >> infra-init/backend.tf
	echo '    prefix = "infra-init/state"' >> infra-init/backend.tf
	echo '  }' >> infra-init/backend.tf
	echo '}' >> infra-init/backend.tf

migrate-state-infra-init:
	cd infra-init && terraform init \
		-backend-config="bucket=$(INFRA_INIT_TF_BUCKET_NAME)" \
		-backend-config="prefix=infra-init/state" \
		-migrate-state
