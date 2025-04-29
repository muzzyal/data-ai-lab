INFRA_INIT_TF_BUCKET_NAME=muz-designed-msc-data-ai-2025-infra-init-tfstate
SA_KEY_FILE = ./infra-init/sa-key.json
SECRET_NAME = GCP_SA_KEY

tf-init-infra-init:
	@cd infra-init && terraform init

tf-plan-infra-init:
	@cd infra-init && terraform plan

tf-apply-infra-init:
	@cd infra-init && terraform apply

migrate-state-infra-init:
	cd infra-init && terraform init -backend-config="bucket=$(INFRA_INIT_TF_BUCKET_NAME)" -backend-config="prefix=infra-init/state" -migrate-state

create-backend-infra-init:
	echo 'terraform {' > infra-init/backend.tf
	echo '  backend "gcs" {' >> infra-init/backend.tf
	echo '    bucket = "$(INFRA_INIT_TF_BUCKET_NAME)"' >> infra-init/backend.tf
	echo '    prefix = "infra-init/state"' >> infra-init/backend.tf
	echo '  }' >> infra-init/backend.tf
	echo '}' >> infra-init/backend.tf

push-sa-key:
	@echo "🔒 Base64 encoding Service Account key..."
	@ENCODED_KEY=$$(cat $(SA_KEY_FILE) | base64 -w 0); \
	echo "🚀 Uploading to GitHub Secret [$(SECRET_NAME)]..."; \
	gh secret set $(SECRET_NAME) --body "$$ENCODED_KEY"
	@echo "🧹 Cleaning up local files..."
	@rm -f $(SA_KEY_FILE)
	@echo "✅ Secret pushed to GitHub and local file removed."
