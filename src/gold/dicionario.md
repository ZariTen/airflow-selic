# Dicionário de Dados | Camada Gold (Dados da SELIC)

A camada Gold consome os dados tratados de taxa SELIC diária da camada Silver, realiza agregações temporais (mensais e anuais) e calcula o rendimento acumulado capitalizado por período, além de métricas de suporte.

---

## 1. Tabela: `acumulado_mensal`

* **Descrição:** Agregação mensal histórica das taxas SELIC diárias.
* **Caminho de Armazenamento:** `/opt/airflow/data/gold/acumulado_mensal/acumulado_mensal.parquet`

| Nome da Coluna | Tipo de Dado | Descrição | Exemplo de Conteúdo |
| :--- | :--- | :--- | :--- |
| **ano** | `int64` | Ano civil de referência da agregação. | `2024` |
| **mes** | `int64` | Mês numérico de referência da agregação (1 a 12). | `5` |
| **taxa_acumulada_mensal** | `float64` | Taxa SELIC acumulada (capitalizada compostamente) no mês, arredondada em 4 casas decimais. | `0.8322` |
| **qtd_dias_uteis** | `int64` | Quantidade total de dias úteis (registros) contabilizados naquele mês. | `21` |

---

## 2. Tabela: `acumulado_anual`

* **Descrição:** Agregação anual histórica das taxas SELIC diárias.
* **Caminho de Armazenamento:** `/opt/airflow/data/gold/acumulado_anual/acumulado_anual.parquet`

| Nome da Coluna | Tipo de Dado | Descrição | Exemplo de Conteúdo |
| :--- | :--- | :--- | :--- |
| **ano** | `int64` | Ano civil de referência da agregação. | `2024` |
| **taxa_acumulada_anual** | `float64` | Taxa SELIC acumulada (capitalizada compostamente) no ano completo, arredondada em 4 casas decimais. | `11.5645` |
| **taxa_media_diaria** | `float64` | Média aritmética simples das taxas diárias do ano, arredondada em 6 casas decimais. | `0.040125` |
| **qtd_dias_uteis** | `int64` | Quantidade total de dias úteis (registros) contabilizados no ano. | `252` |

---
