PYTHON ?= python3
VENV   ?= .venv
ifeq ($(OS),Windows_NT)
PY      = $(VENV)/Scripts/python
else
PY      = $(VENV)/bin/python
endif

.PHONY: setup infra alerta perfil perfil-sem-ops investigar bronze silver gold pipeline produtor micro-lote testes

setup:            ## cria o ambiente virtual e instala dependências
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

infra:            ## cria datasets, tabelas ops, bucket e tópico no GCP
	bash infra/setup.sh

alerta:           ## canal de e-mail + política de alerta do Cloud Monitoring (ALERT_EMAIL)
	$(PY) -m scripts.criar_alerta

perfil:           ## perfil da fonte, gravando em ops.execucoes
	$(PY) -m scripts.perfil_fonte

perfil-sem-ops:   ## perfil da fonte sem depender do make infra
	$(PY) -m scripts.perfil_fonte --sem-ops

investigar:       ## consultas que respondem as divergências do perfil
	$(PY) -m scripts.investigar_fonte

bronze:           ## carga append das 8 tabelas em bronze.*
	$(PY) -m pipeline.orquestrador --camadas bronze

silver:           ## Silver com regras de qualidade antes de publicar
	$(PY) -m pipeline.orquestrador --camadas silver

gold:             ## Gold com regras e exportação Parquet para o bucket
	$(PY) -m pipeline.orquestrador --camadas gold

pipeline:         ## todas as camadas implementadas, em ordem
	$(PY) -m pipeline.orquestrador

produtor:         ## streaming: publica eventos simulados no Pub/Sub (5/s por 2 min)
	$(PY) -m streaming.produtor --eventos-por-segundo 5 --duracao-s 120

micro-lote:       ## streaming: 10 micro-lotes, um por minuto
	$(PY) -m streaming.micro_lote --lotes 10 --intervalo-s 60

testes:           ## testes automatizados (não precisam do GCP)
	$(PY) -m pytest -q
