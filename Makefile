# Variáveis organizacionais
CLUSTER_NAME=kind
IMAGE_NAME=meu-airflow:local

.PHONY: up down status logs clean

up: ## Cria o cluster (se necessário), faz build e sobe todo o ambiente de forma automática
	@echo "Iniciando ambiente Kubernetes..."
	@if ! kind get clusters | grep -q "^kind$$"; then \
		echo "Cluster Kind nao encontrado. Criando."; \
		kind create cluster --config k8s/kind-config.yaml; \
	else \
		echo "Cluster Kind ja existe. Pulando criacao."; \
	fi
	@echo "Build da imagem Docker..."
	@docker build --no-cache -t $(IMAGE_NAME) .
	@echo "Carregando imagem no cluster Kind (pode levar ate 1 minuto)."
	@kind load docker-image $(IMAGE_NAME)
	@echo "Aplicando infraestrutura base (ConfigMap, Volumes, Postgres)."
	@kubectl apply -f k8s/base/
	@echo "Aguardando Postgres ficar pronto..."
	@kubectl wait --for=condition=ready pod -l app=postgres --timeout=60s
	@echo "Executando migracoes do banco de dados (Airflow Init)."
	@kubectl apply -f k8s/apps/airflow-init-job.yaml
	@kubectl wait --for=condition=complete job/airflow-init --timeout=90s
	@echo "Subindo servicos do Airflow (API, Scheduler e DAG Processor)."
	@kubectl apply -f k8s/apps/airflow-services.yaml

down: ## Derruba os serviços do Airflow sem apagar o cluster
	@echo "Removendo servicos do Airflow..."
	@kubectl delete -f k8s/apps/ || true
	@kubectl delete -f k8s/base/ || true

clean: ## Destrói o cluster Kind completamente
	@echo "Destruindo cluster Kubernetes..."
	@kind delete cluster

status: ## Verifica a saúde dos Pods no cluster
	@kubectl get pods
