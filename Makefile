# Variáveis organizacionais
CLUSTER_NAME   ?= kind
IMAGE_NAME     ?= meu-airflow:local
KIND_CONFIG    ?= k8s/kind-config.yaml
BASE_MANIFESTS ?= k8s/base/
APP_MANIFESTS  ?= k8s/apps/

# Alvos virtuais (não geram arquivos)
.PHONY: help up cluster build rebuild load deploy-base wait-postgres \
        migrate deploy-services down clean status logs

help: ## Exibe esta ajuda
	@echo "Alvos disponíveis:"
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

# ---------- Ciclo de vida ----------
up: cluster build load deploy-base wait-postgres migrate deploy-services ## Sobe o ambiente completo
	@echo "Ambiente Airflow pronto no cluster '$(CLUSTER_NAME)'."

cluster: ## Cria o cluster Kind se ainda não existir
	@if ! kind get clusters | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "Cluster '$(CLUSTER_NAME)' não encontrado. Criando..."; \
		kind create cluster --config $(KIND_CONFIG); \
	else \
		echo "Cluster '$(CLUSTER_NAME)' já existe. Pulando criação."; \
	fi

build: ## Build da imagem Docker (usa cache de camadas)
	docker build -t $(IMAGE_NAME) .

rebuild: ## Build limpo (--no-cache)
	docker build --no-cache -t $(IMAGE_NAME) .

load: ## Carrega a imagem no cluster Kind
	kind load docker-image $(IMAGE_NAME) --name $(CLUSTER_NAME)

deploy-base: ## Aplica ConfigMap, Volumes e Postgres
	kubectl apply -f $(BASE_MANIFESTS)

wait-postgres: ## Aguarda o Postgres ficar pronto
	kubectl wait --for=condition=ready pod -l app=postgres --timeout=60s

migrate: ## Roda as migrações (airflow-init job)
	kubectl apply -f $(APP_MANIFESTS)airflow-init-job.yaml
	kubectl wait --for=condition=complete job/airflow-init --timeout=90s

deploy-services: ## Sobe API, Scheduler e DAG Processor
	kubectl apply -f $(APP_MANIFESTS)airflow-services.yaml

# ---------- Operação ----------
down: ## Remove serviços sem apagar o cluster
	@echo "Removendo serviços do Airflow..."
	-kubectl delete -f $(APP_MANIFESTS)
	-kubectl delete -f $(BASE_MANIFESTS)

clean: ## Destrói o cluster Kind completamente
	@echo "Destruindo cluster Kubernetes..."
	kind delete cluster --name $(CLUSTER_NAME)

status: ## Mostra o estado dos Pods
	kubectl get pods

logs: ## Mostra logs dos pods do Airflow
	kubectl logs -l app=airflow -f
