"""Regressões dos volumes persistentes nas configurações de implantação."""

from pathlib import Path

import yaml


RAIZ = Path(__file__).resolve().parents[1]
CAMINHO_DADOS = "/opt/airflow/data"


def carregar_yaml(caminho):
    """Carrega os documentos YAML reais, resolvendo também os anchors."""
    with (RAIZ / caminho).open(encoding="utf-8") as arquivo:
        return list(yaml.safe_load_all(arquivo))


def test_compose_compartilha_volume_nomeado_persistente_pelo_anchor():
    compose = carregar_yaml("docker-compose.yaml")[0]
    comum = compose["x-airflow-common"]
    montagens = [
        volume for volume in comum["volumes"]
        if volume.split(":")[1] == CAMINHO_DADOS
    ]
    assert len(montagens) == 1, "O anchor deve montar o volume de dados."
    origem = montagens[0].split(":")[0]
    assert origem in compose["volumes"], "Os dados precisam de um volume nomeado."
    assert not origem.startswith((".", "/")), "Não usar bind mount para os dados."
    for nome, servico in compose["services"].items():
        if nome.startswith("airflow-"):
            assert servico["volumes"] is comum["volumes"], (
                f"{nome} deve herdar todos os volumes do anchor."
            )
            assert montagens[0] in servico["volumes"]
            if nome != "airflow-init":
                assert servico["user"] == "${AIRFLOW_UID:-50000}:0"


def test_compose_inicializa_permissoes_do_volume_de_dados():
    compose = carregar_yaml("docker-compose.yaml")[0]
    inicializacao = compose["services"]["airflow-init"]
    assert inicializacao["user"] == "0:0"
    comando = inicializacao["command"][-1]
    assert "mkdir -p /opt/airflow/data" in comando
    assert 'chown "${AIRFLOW_UID:-50000}:0" /opt/airflow/data' in comando
    assert "chmod 775 /opt/airflow/data" in comando


def test_kubernetes_compartilha_pvc_de_dados_com_permissoes():
    volumes = carregar_yaml("k8s/base/volumes.yaml")
    pvcs = {doc["metadata"]["name"]: doc for doc in volumes}
    assert "airflow-data-pvc" in pvcs
    assert pvcs["airflow-data-pvc"]["spec"]["accessModes"] == ["ReadWriteOnce"]
    for doc in carregar_yaml("k8s/apps/airflow-services.yaml"):
        if doc["kind"] != "Deployment":
            continue
        pod = doc["spec"]["template"]["spec"]
        assert pod["securityContext"] == {"runAsUser": 50000, "fsGroup": 0}
        nomes = {v["name"] for v in pod["volumes"]
                 if v.get("persistentVolumeClaim", {}).get("claimName") == "airflow-data-pvc"}
        assert nomes, f"{doc['metadata']['name']} precisa do PVC compartilhado."
        for container in pod["containers"]:
            assert any(m["name"] in nomes and m["mountPath"] == CAMINHO_DADOS
                       for m in container["volumeMounts"])


def test_down_preserva_volumes_persistentes():
    makefile = (RAIZ / "Makefile").read_text(encoding="utf-8")
    alvo = makefile.split("\ndown:", 1)[1].split("\nclean:", 1)[0]
    assert "delete -f $(BASE_MANIFESTS)\n" not in alvo
    assert "$(BASE_MANIFESTS)postgres.yaml" in alvo
    assert "$(BASE_MANIFESTS)configmap.yaml" in alvo


def test_imagem_de_testes_inclui_configuracoes_verificadas():
    dockerfile = (RAIZ / "Dockerfile.test").read_text(encoding="utf-8")
    assert "COPY docker-compose.yaml Makefile Dockerfile.test /opt/airflow/" in dockerfile
    assert "COPY k8s /opt/airflow/k8s" in dockerfile
