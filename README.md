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
| `make up` | Cria o cluster Kind (se necessário), build da imagem, injeta no cluster e sobe toda a stack |
| `make down` | Derruba os serviços e a infraestrutura, mantendo o cluster |
| `make clean` | Destroi o cluster Kind completamente |
| `make status` | Lista os pods e seus status atuais |
| `make help` | Exibe todos os comandos disponíveis |

**Subir o ambiente completo:**

```bash
make up
```

**Verificar se os pods estão saudáveis:**

```bash
make status
```

**Derrubar os serviços (sem apagar o cluster):**

```bash
make down
```

**Limpeza total (apaga o cluster Kind):**

```bash
make clean
```
