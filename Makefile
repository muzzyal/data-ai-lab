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

create-docker-image:
	docker build -t $(APP) -f cloud-dock/$(APP)/Dockerfile  cloud-dock

run-docker-image:
	docker run \
		-e GOOGLE_CLOUD_PROJECT=muz-designed-msc-data-ai-2025 \
		-e PUBSUB_TOPIC_NAME=playground_project_topic \
		-e DLQ_TOPIC_NAME=playground_project_dlq \
		-e SECRET_ID=playground_project_stream_secret \
		-e GOOGLE_APPLICATION_CREDENTIALS=/home/.config/gcloud/application_default_credentials.json \
		-e PORT=3000 \
		-p 3000:3000 \
		-v ~/.config/gcloud:/home/.config/gcloud \
		${APP}

test-coverage:
	python3 -m pytest --cov=$(APP).src --cov-report=xml --cov-report=html --cov-report=term --cov-branch

ecommerce-simulator:
	@echo "Running ecommerce simulator..."
	@cd ecommerce-simulator && python3 app.py

.PHONY: tf-init-infra-init tf-plan-infra-init tf-apply-infra-init create-backend-infra-init migrate-state-infra-init create-docker-image run-docker-image test-coverage ecommerce-simulator
