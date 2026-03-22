# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  utils/helpers.py
#  Funções auxiliares reutilizadas por todos os outros módulos do projeto.
# ─────────────────────────────────────────────────────────────────────────────

import os
import logging
import colorlog
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()


# ─────────────────────────────────────────
#  1. CONFIGURAÇÃO DE LOGS
# ─────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    """
    Cria e retorna um logger com saída colorida no terminal e em arquivo.

    Como usar em outros módulos:
        from utils.helpers import setup_logger
        logger = setup_logger(__name__)
        logger.info("Coleta iniciada")
        logger.error("Falha ao conectar")

    Args:
        name: Nome do logger — sempre passe __name__ para identificar o módulo.

    Returns:
        logging.Logger configurado com handlers de terminal e arquivo.
    """
    log_path = os.getenv("LOG_PATH", "logs/app.log")
    log_level_str = os.getenv("LOG_LEVEL", "INFO")
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # Garante que a pasta logs/ existe
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger(name)

    # Evita duplicar handlers se o logger já foi configurado
    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    # ── Handler 1: terminal com cores ──
    terminal_formatter = colorlog.ColoredFormatter(
        "%(log_color)s[%(asctime)s] %(levelname)-8s%(reset)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        }
    )
    terminal_handler = logging.StreamHandler()
    terminal_handler.setFormatter(terminal_formatter)

    # ── Handler 2: arquivo de log (sem cores) ──
    file_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(file_formatter)

    logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)

    return logger


# ─────────────────────────────────────────
#  2. LIMPEZA E NORMALIZAÇÃO DE HASHTAGS
# ─────────────────────────────────────────

def normalize_hashtag(hashtag: str) -> str:
    """
    Padroniza o nome de uma hashtag para armazenamento no banco.

    Regras aplicadas:
      - Remove espaços extras nas bordas
      - Converte para minúsculas
      - Remove o símbolo # se o usuário digitou com ele

    Exemplos:
        "#Gaming"   → "gaming"
        " #FYP "    → "fyp"
        "valorant"  → "valorant"

    Args:
        hashtag: String digitada pelo usuário.

    Returns:
        String limpa e padronizada, sem o #.
    """
    return hashtag.strip().lstrip("#").lower()


def format_hashtag_display(hashtag: str) -> str:
    """
    Formata a hashtag para exibição na interface (com # na frente).

    Exemplo:
        "gaming" → "#gaming"

    Args:
        hashtag: Hashtag já normalizada (sem #).

    Returns:
        String com # para exibir ao usuário.
    """
    cleaned = normalize_hashtag(hashtag)
    return f"#{cleaned.capitalize()}" 


# ─────────────────────────────────────────
#  3. FORMATAÇÃO DE NÚMEROS GRANDES
# ─────────────────────────────────────────

def format_number(value: float | int | None) -> str:
    """
    Converte números grandes em formato legível.

    Exemplos:
        22_000_000_000 → "22.0B"
        3_500_000      → "3.5M"
        85_000         → "85.0K"
        999            → "999"
        None           → "N/A"

    Args:
        value: Número inteiro ou float. Aceita None sem quebrar.

    Returns:
        String formatada com sufixo B/M/K ou "N/A" se o valor for None.
    """
    if value is None:
        return "N/A"
    value = float(value)
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return str(int(value))


def format_percentage(value: float | None, decimals: int = 1) -> str:
    """
    Formata um número como porcentagem com sinal de + ou -.

    Exemplos:
        4.7   → "+4.7%"
        -2.3  → "-2.3%"
        0.0   → "0.0%"
        None  → "N/A"

    Args:
        value:    Número representando a variação percentual.
        decimals: Casas decimais (padrão: 1).

    Returns:
        String formatada com sinal e símbolo %.
    """
    if value is None:
        return "N/A"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimals}f}%"


# ─────────────────────────────────────────
#  4. DATA E HORA
# ─────────────────────────────────────────

def now_str() -> str:
    """
    Retorna a data e hora atual formatada para salvar no banco.

    Returns:
        String no formato "YYYY-MM-DD HH:MM:SS".
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_datetime_display(dt_str: str) -> str:
    """
    Converte uma string de data do banco para formato legível na interface.

    Exemplo:
        "2026-03-11 15:32:00" → "11/03/2026 às 15:32"

    Args:
        dt_str: String de data no formato "YYYY-MM-DD HH:MM:SS".

    Returns:
        String formatada para exibição, ou a original se falhar.
    """
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y às %H:%M")
    except (ValueError, TypeError):
        return dt_str or "Data desconhecida"


# ─────────────────────────────────────────
#  5. VARIÁVEIS DE AMBIENTE (atalhos)
# ─────────────────────────────────────────

def get_db_path() -> str:
    """Retorna o caminho do banco de dados definido no .env."""
    return os.getenv("DATABASE_PATH", "data/trends.db")


def get_request_timeout() -> int:
    """Retorna o timeout de requisições HTTP definido no .env."""
    return int(os.getenv("REQUEST_TIMEOUT", 15))


def get_request_delay() -> float:
    """Retorna o delay entre requisições HTTP definido no .env."""
    return float(os.getenv("REQUEST_DELAY", 2))


def get_max_hashtags() -> int:
    """Retorna o limite máximo de hashtags monitoradas definido no .env."""
    return int(os.getenv("MAX_HASHTAGS", 100))


def get_collection_interval() -> int:
    """
    Retorna o intervalo de coleta em horas definido no .env.
    0 significa coleta manual (sem agendamento automático).
    """
    return int(os.getenv("COLLECTION_INTERVAL_HOURS", 0))


# ─────────────────────────────────────────
#  6. VALIDAÇÕES
# ─────────────────────────────────────────

def is_valid_hashtag(hashtag: str) -> bool:
    """
    Verifica se uma string é uma hashtag válida para monitorar.

    Regras:
      - Não pode ser vazia
      - Deve ter ao menos 2 caracteres (sem o #)
      - Só pode conter letras, números e underscores

    Exemplos:
        "#gaming"      → True
        "fyp"          → True
        ""             → False
        "#a"           → False (muito curta)
        "#hello world" → False (espaço inválido)

    Args:
        hashtag: String digitada pelo usuário (com ou sem #).

    Returns:
        True se válida, False caso contrário.
    """
    cleaned = normalize_hashtag(hashtag)
    if len(cleaned) < 2:
        return False
    # Aceita letras (incluindo acentuadas), números e underscore
    import re
    return bool(re.match(r'^[\w]+$', cleaned, re.UNICODE))