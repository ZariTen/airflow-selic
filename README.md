# Pipeline SELIC

Pipeline da taxa SELIC via API do BCB, orquestrado com Apache Airflow em arquitetura medalhão (Bronze → Silver → Gold).

A `master_orquestrador_bcb` executa diariamente às 10h (`America/Sao_Paulo`): validação da API → ingestão → validação Gold. A carga vai de 01/01/2020 até hoje, conforme a disponibilidade dos dados no BCB.

## Estrutura

```text
dags/
├── master_dag_selic.py       # orquestração
├── sub_validacao_api.py      # disponibilidade da API
├── sub_ingestao_selic.py     # Bronze → Silver → Gold
└── sub_validacao_gold.py     # qualidade e cobertura temporal
src/
├── periodo_selic.py          # período da carga
├── bronze/ingerir_selic.py   # extração e carga raw
├── silver/transform_selic.py # limpeza, tipagem e quarentena
└── gold/agregar_selic.py     # agregações mensais e anuais
```

## Decisões técnicas

- Cada etapa tem uma DAG independente, acionada por `TriggerDagRunOperator` com `wait_for_completion=True`. Isso isola falhas e permite reexecutar a etapa afetada. Só a master é agendada; cada DAG permite uma execução ativa.
- Os `BashOperator` executam scripts Python independentes, permitindo testar cada camada sem Airflow.
- A Silver separa registros inválidos (data nula, valor fora de faixa, duplicata) em `data/quarentena/selic_rejeitados.parquet`, com o motivo da rejeição, acumulando entre execuções para auditoria. Os válidos ficam em Parquet particionado por `ano=YYYY`, reduzindo I/O nas leituras seguintes.
- Cada carga reprocessa o período completo e sobrescreve as partições anuais. Consultas superiores ao limite do BCB são divididas em janelas inferiores a dez anos.
- O `HttpSensor` consulta os últimos 14 dias em `mode=reschedule`, sem ocupar um worker enquanto espera. Os retries usam backoff exponencial com teto por criticidade.
- A Gold verifica arquivos, campos nulos e cobertura de anos e meses, considerando fins de semana, feriados bancários e a possível ausência do dia final ainda não publicado.
- Os serviços compartilham `/opt/airflow/data` em volume persistente. No Kind de um nó, o PVC usa `ReadWriteOnce`; múltiplos nós exigem consumidores no mesmo nó ou armazenamento `ReadWriteMany`.

## Como rodar

### Docker Compose

```bash
docker-compose up --build -d
```

Acesse `http://localhost:8080`: usuário `airflow`, senha gerada no container `airflow-api-server-1`. A conexão com o BCB já vem configurada.

Para disparar manualmente ou testar uma camada isolada:

```bash
docker exec airflow-selic-airflow-scheduler-1 airflow dags trigger master_orquestrador_bcb
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/bronze/ingerir_selic.py
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/silver/transform_selic.py
docker exec airflow-selic-airflow-scheduler-1 python /opt/airflow/src/gold/agregar_selic.py
```

`docker-compose down` preserva os dados. **`down -v` apaga os volumes de dados e do Postgres.**

### Kubernetes (Kind)

Requer `kind`, `kubectl` e `docker` no PATH. Use `make up` para subir tudo; as etapas também podem ser executadas separadamente:

| Comando | Função |
|---|---|
| `make cluster` | Cria o cluster, se necessário |
| `make build` / `make rebuild` | Build com cache / sem cache |
| `make load` | Carrega a imagem no Kind |
| `make deploy-base` | Aplica ConfigMap, volumes e Postgres |
| `make wait-postgres` | Aguarda o Postgres |
| `make migrate` | Executa as migrações |
| `make deploy-services` | Sobe API, Scheduler e DAG Processor |
| `make status` / `make logs` | Consulta pods / acompanha logs |
| `make k8s-test` | Executa os testes em um Job |
| `make down` | Remove serviços, preservando cluster e PVCs |
| `make clean` | **Destrói o cluster e seus dados locais** |
| `make help` | Lista todos os comandos |

### Período histórico

Configure `SELIC_DATA_INICIAL` e `SELIC_DATA_FINAL` (`DD/MM/AAAA`) no ambiente dos serviços: `x-airflow-common.environment` no Compose ou ConfigMap no Kubernetes. Sem elas, vale o período padrão até hoje. Datas inválidas, intervalo invertido e término futuro são rejeitados. Use outro volume para não misturar cargas históricas com a diária.

## Testes

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-test.txt
python -m pytest -q
```

Os testes verificam as camadas e os manifests sem subir serviços. Com Airflow instalado, também carregam as DAGs e renderizam o sensor; sem ele, esses testes são ignorados. Para incluir consultas reais ao BCB:

```bash
SELIC_TESTE_API_REAL=1 python -m pytest -q tests/test_atualizacao_real.py
```
