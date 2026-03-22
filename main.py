# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  main.py
#  Ponto de entrada do programa.
#
#  Como executar:
#      python -m streamlit run main.py
#
#  IMPORTANTE: st.set_page_config() DEVE ser a primeira chamada Streamlit.
#  Por isso ela fica aqui no main.py, antes de qualquer outro comando.
# ─────────────────────────────────────────────────────────────────────────────

import os
import streamlit as st

# ── set_page_config PRIMEIRO — antes de qualquer import que use streamlit ──
st.set_page_config(
    page_title="Trend Monitor",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

from database.database_manager import initialize_database
from ui.dashboard import run_dashboard
from utils.helpers import setup_logger

logger = setup_logger(__name__)


# ─────────────────────────────────────────
#  INICIALIZAÇÃO (roda só uma vez)
# ─────────────────────────────────────────

@st.cache_resource
def _initialize() -> None:
    """
    Inicializa o sistema na primeira execução.

    O @st.cache_resource garante que essa função rode apenas UMA vez
    por sessão do servidor — mesmo que o Streamlit re-execute o main.py
    dezenas de vezes por causa de interações do usuário.
    """
    logger.info("━" * 50)
    logger.info("  Trend Monitor — Iniciando...")
    logger.info("━" * 50)

    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    initialize_database()

    logger.info("Sistema pronto.")


# ─────────────────────────────────────────
#  EXECUÇÃO
# ─────────────────────────────────────────

_initialize()
run_dashboard()