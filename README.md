# Pipeline SELIC

Pipeline de ingestão e transformação da taxa SELIC via API do BCB, orquestrado com Apache Airflow em arquitetura medalhão (Bronze -> Silver -> Gold).

## Estrutura

```
dags/
├── master_dag_selic.py       # orquestradora principal
├── sub_validacao_api.py      # verifica disponibilidade da API
├── sub_ingestao_selic.py     # pipeline Bronze -> Silver -> Gold
└── sub_validacao_gold.py     # quality check final

src/
├── bronze/ingerir_selic.py   # extração e carga raw
├── silver/transform_selic.py # limpeza, tipagem e quarentena
└── gold/agregar_selic.py     # agregações analíticas
```

O fluxo é sempre disparado pela `master_orquestrador_bcb`, que aciona em sequência: validação da API, pipeline de ingestão e validação da camada Gold.

## Decisões técnicas

Cada etapa do pipeline vive em uma DAG independente, disparada via `TriggerDagRunOperator` com `wait_for_completion=True`. Isso isola falhas por domínio e permite reexecutar só a etapa que falhou sem reprocessar tudo.

Na Silver, registros inválidos não quebram o job eles vão para `data/quarentena/selic_rejeitados.parquet` com o motivo anotado (data nula, valor fora de range, duplicata), acumulando entre execuções para auditoria. Os dados válidos são particionados por `ano=YYYY` em Parquet, o que reduz I/O nas leituras downstream.

Os `BashOperator` da sub de ingestão executam scripts Python independentes, o que permite testar cada camada isoladamente sem precisar do Airflow no meio do caminho.

O `HttpSensor` roda em `mode=reschedule` para não segurar slot de worker enquanto aguarda a API responder, e os retries usam backoff exponencial com teto calibrado por criticidade.

## Como rodar

```bash
docker-compose up --build -d
```

Acesse `http://localhost:8080` com usuário `airflow` e senha gerada no container `airflow-api-server-1`. A connection com a API do BCB já está configurada via variável de ambiente no compose e não precisa de nenhuma configuração manual no UI.

Dispare a DAG `master_orquestrador_bcb` pelo UI ou via CLI:

```bash
docker exec airflow-selic-airflow-scheduler-1 airflow dags trigger master_orquestrador_bcb
```

Para testar uma camada isoladamente:

```bash
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/bronze/ingerir_selic.py
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/silver/transform_selic.py
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/gold/agregar_selic.py
```

Para parar: `docker-compose down` (ou `down -v` para apagar o volume do Postgres também).

### Kubernetes (Makefile)

Pré-requisitos: [`kind`](https://kind.sigs.k8s.io/), `kubectl` e `docker` instalados e disponíveis no PATH.

| Comando | O que faz |
|---|---|
| `make up` | Sobe o ambiente completo |
| `make cluster` | Cria o cluster Kind se ainda não existir |
| `make build` | Build da imagem Docker (aproveita cache de camadas) |
| `make rebuild` | Build limpo sem cache (`--no-cache`) |
| `make load` | Carrega a imagem no cluster Kind |
| `make deploy-base` | Aplica ConfigMap, Volumes e Postgres |
| `make wait-postgres` | Aguarda o Pod do Postgres ficar pronto |
| `make migrate` | Executa as migrações via `airflow-init` job |
| `make deploy-services` | Sobe API, Scheduler e DAG Processor |
| `make down` | Remove serviços e base, mantendo o cluster |
| `make clean` | Destrói o cluster Kind completamente |
| `make status` | Lista os pods e seus status atuais |
| `make logs` | Segue os logs de todos os pods do Airflow |
| `make help` | Exibe todos os comandos disponíveis |

**Subir o ambiente completo:**

```bash
make up
```

Cada etapa do pipeline acima também pode ser executada individualmente:

```bash
make cluster        # só cria o cluster
make build          # só builda a imagem
make rebuild        # build sem cache
make deploy-base    # só aplica os manifests base
make migrate        # só roda as migrações
make deploy-services # só sobe os serviços Airflow
```

**Verificar se os pods estão saudáveis:**

```bash
make status
```

**Acompanhar os logs:**

```bash
make logs
```

**Derrubar os serviços (sem apagar o cluster):**

```bash
make down
```

**Limpeza total (apaga o cluster Kind):**

```bash
make clean
```
