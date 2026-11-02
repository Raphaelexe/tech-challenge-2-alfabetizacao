PYTHON ?= python3
VENV   ?= .venv
ifeq ($(OS),Windows_NT)
PY      = $(VENV)/Scripts/python
else
PY      = $(VENV)/bin/python
endif

.PHONY: setup infra perfil perfil-sem-ops investigar bronze silver pipeline testes

setup:            ## cria o ambiente virtual e instala dependências
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

infra:            ## cria datasets, tabelas ops, bucket e tópico no GCP
	bash infra/setup.sh

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

pipeline:         ## todas as camadas implementadas, em ordem
	$(PY) -m pipeline.orquestrador

testes:           ## testes automatizados (não precisam do GCP)
	$(PY) -m pytest -q
