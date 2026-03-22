# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  database/database_manager.py
#  Gerencia toda a comunicação com o banco de dados SQLite.
#  Nenhum outro módulo deve escrever SQL diretamente — tudo passa por aqui.
# ─────────────────────────────────────────────────────────────────────────────
 
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from unittest import result
 
from utils.helpers import setup_logger, get_db_path, normalize_hashtag
 
logger = setup_logger(__name__)
 
 
# ─────────────────────────────────────────
#  1. CONEXÃO E CRIAÇÃO DO BANCO
# ─────────────────────────────────────────
 
def get_connection() -> sqlite3.Connection:
    """
    Abre e retorna uma conexão com o banco SQLite.
 
    - Garante que a pasta data/ existe antes de tentar criar o arquivo.
    - Ativa o modo WAL (Write-Ahead Logging) para melhor desempenho
      quando múltiplas leituras e escritas acontecem ao mesmo tempo.
    - row_factory permite acessar colunas pelo nome (row["hashtag"])
      em vez de por índice (row[1]).
 
    Returns:
        sqlite3.Connection pronta para uso.
    """
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
 
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row          # acesso por nome de coluna
    conn.execute("PRAGMA journal_mode=WAL") # melhor desempenho
    conn.execute("PRAGMA foreign_keys=ON")  # integridade referencial
    return conn
 
 
def initialize_database() -> None:
    """
    Cria as tabelas do banco se ainda não existirem.
 
    Deve ser chamada UMA VEZ na inicialização do programa (em main.py).
    É seguro chamar múltiplas vezes — o IF NOT EXISTS evita erros.
 
    Tabelas criadas:
        hashtags       — lista de hashtags sendo monitoradas
        hashtag_stats  — histórico de coletas de cada hashtag
    """
    logger.info("Inicializando banco de dados...")
 
    conn = get_connection()
    cursor = conn.cursor()
 
    # ── Tabela 1: hashtags monitoradas ──────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtags (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            hashtag    TEXT    NOT NULL UNIQUE,  -- ex: "gaming" (sem #)
            created_at TEXT    NOT NULL          -- "YYYY-MM-DD HH:MM:SS"
        )
    """)
 
    # ── Tabela 2: histórico de estatísticas coletadas ────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_stats (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            hashtag       TEXT    NOT NULL,
            views_total   INTEGER,   -- total de visualizações da hashtag
            videos_total  INTEGER,   -- número de vídeos com essa hashtag
            avg_views     REAL,      -- média de views dos vídeos recentes
            avg_likes     REAL,      -- média de curtidas dos vídeos recentes
            avg_comments  REAL,      -- média de comentários dos vídeos recentes
            collected_at  TEXT NOT NULL,  -- "YYYY-MM-DD HH:MM:SS"
 
            FOREIGN KEY (hashtag) REFERENCES hashtags(hashtag)
        )
    """)
 
    # Índice para acelerar buscas por hashtag + data (muito usadas nos gráficos)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_stats_hashtag_date
        ON hashtag_stats (hashtag, collected_at)
    """)
 
    conn.commit()
    conn.close()
    logger.info("Banco de dados pronto.")
 
 
# ─────────────────────────────────────────
#  2. OPERAÇÕES COM HASHTAGS MONITORADAS
# ─────────────────────────────────────────
 
def add_hashtag(hashtag: str) -> dict:
    """
    Adiciona uma hashtag à lista de monitoramento.
 
    Args:
        hashtag: String com ou sem # (ex: "#gaming" ou "gaming").
 
    Returns:
        dict com:
            success (bool)  — True se adicionou, False se já existia ou erro
            message (str)   — mensagem para exibir ao usuário
    """
    cleaned = normalize_hashtag(hashtag)
 
    try:
        conn = get_connection()
        conn.execute(
            "INSERT INTO hashtags (hashtag, created_at) VALUES (?, ?)",
            (cleaned, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
        conn.close()
        logger.info(f"Hashtag adicionada: #{cleaned}")
        return {"success": True, "message": f"#{cleaned} adicionada com sucesso!"}
 
    except sqlite3.IntegrityError:
        # UNIQUE constraint — hashtag já existe
        logger.warning(f"Hashtag já monitorada: #{cleaned}")
        return {"success": False, "message": f"#{cleaned} já está sendo monitorada."}
 
    except Exception as e:
        logger.error(f"Erro ao adicionar hashtag #{cleaned}: {e}")
        return {"success": False, "message": f"Erro ao adicionar #{cleaned}: {str(e)}"}
 
 
def remove_hashtag(hashtag: str) -> dict:
    """
    Remove uma hashtag e todo o seu histórico de estatísticas.
 
    Args:
        hashtag: Nome da hashtag (com ou sem #).
 
    Returns:
        dict com success (bool) e message (str).
    """
    cleaned = normalize_hashtag(hashtag)
 
    try:
        conn = get_connection()
 
        # Remove o histórico primeiro (respeita a foreign key)
        conn.execute("DELETE FROM hashtag_stats WHERE hashtag = ?", (cleaned,))
 
        # Depois remove a hashtag em si
        cursor = conn.execute(
            "DELETE FROM hashtags WHERE hashtag = ?", (cleaned,)
        )
        conn.commit()
        conn.close()
 
        if cursor.rowcount == 0:
            return {"success": False, "message": f"#{cleaned} não encontrada."}
 
        logger.info(f"Hashtag removida: #{cleaned}")
        return {"success": True, "message": f"#{cleaned} removida com sucesso!"}
 
    except Exception as e:
        logger.error(f"Erro ao remover hashtag #{cleaned}: {e}")
        return {"success": False, "message": f"Erro: {str(e)}"}
 
 
def get_all_hashtags() -> list[dict]:
    """
    Retorna todas as hashtags sendo monitoradas.
 
    Returns:
        Lista de dicts com campos: id, hashtag, created_at
        Exemplo: [{"id": 1, "hashtag": "gaming", "created_at": "2026-03-11 15:00:00"}]
    """
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT id, hashtag, created_at FROM hashtags ORDER BY created_at DESC"
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]
 
    except Exception as e:
        logger.error(f"Erro ao buscar hashtags: {e}")
        return []
 
 
def hashtag_exists(hashtag: str) -> bool:
    """
    Verifica se uma hashtag já está cadastrada no banco.
 
    Args:
        hashtag: Nome da hashtag (com ou sem #).
 
    Returns:
        True se existe, False caso contrário.
    """
    cleaned = normalize_hashtag(hashtag)
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT 1 FROM hashtags WHERE hashtag = ?", (cleaned,)
        ).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        logger.error(f"Erro ao verificar hashtag: {e}")
        return False
 
 
# ─────────────────────────────────────────
#  3. OPERAÇÕES COM ESTATÍSTICAS
# ─────────────────────────────────────────
 
def save_stats(stats: dict) -> bool:
    """
    Salva uma coleta de estatísticas no banco.
 
    Chamado pelo scraper após cada coleta bem-sucedida.
 
    Args:
        stats: Dicionário com os dados coletados. Campos esperados:
            hashtag      (str)            — obrigatório
            views_total  (int | None)
            videos_total (int | None)
            avg_views    (float | None)
            avg_likes    (float | None)
            avg_comments (float | None)
            collected_at (str | None)     — usa now() se não fornecido
 
    Returns:
        True se salvou com sucesso, False se houve erro.
 
    Exemplo de uso:
        save_stats({
            "hashtag": "gaming",
            "views_total": 22_000_000_000,
            "videos_total": 3_500_000,
            "avg_views": 50_000,
            "avg_likes": 1_200,
            "avg_comments": 85,
        })
    """
    try:
        hashtag = normalize_hashtag(stats.get("hashtag", ""))
        if not hashtag:
            logger.warning("save_stats chamado sem hashtag.")
            return False
 
        collected_at = stats.get(
            "collected_at",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
 
        conn = get_connection()
        conn.execute("""
            INSERT INTO hashtag_stats
                (hashtag, views_total, videos_total, avg_views,
                 avg_likes, avg_comments, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            hashtag,
            stats.get("views_total"),
            stats.get("videos_total"),
            stats.get("avg_views"),
            stats.get("avg_likes"),
            stats.get("avg_comments"),
            collected_at,
        ))
        conn.commit()
        conn.close()
        logger.info(f"Stats salvas para #{hashtag} em {collected_at}")
        return True
 
    except Exception as e:
        logger.error(f"Erro ao salvar stats: {e}")
        return False
 
 
def get_stats_history(hashtag: str, limit: int = 100) -> list[dict]:
    """
    Retorna o histórico de coletas de uma hashtag, do mais recente ao mais antigo.
 
    Usado pelos gráficos de tendência na interface.
 
    Args:
        hashtag: Nome da hashtag (com ou sem #).
        limit:   Número máximo de registros retornados (padrão: 100).
 
    Returns:
        Lista de dicts com todos os campos de hashtag_stats.
    """
    cleaned = normalize_hashtag(hashtag)
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT id, hashtag, views_total, videos_total,
                   avg_views, avg_likes, avg_comments, collected_at
            FROM hashtag_stats
            WHERE hashtag = ?
            ORDER BY datetime(collected_at) ASC
            LIMIT ?
        """, (cleaned, limit)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
 
    except Exception as e:
        logger.error(f"Erro ao buscar histórico de #{cleaned}: {e}")
        return []
 
 
def get_latest_stats(hashtag: str) -> Optional[dict]:
    """
    Retorna apenas a coleta mais recente de uma hashtag.
 
    Usado para exibir o card de resumo na interface.
 
    Args:
        hashtag: Nome da hashtag (com ou sem #).
 
    Returns:
        Dict com os dados mais recentes, ou None se não houver registros.
    """
    cleaned = normalize_hashtag(hashtag)
    try:
        conn = get_connection()
        row = conn.execute("""
            SELECT id, hashtag, views_total, videos_total,
                   avg_views, avg_likes, avg_comments, collected_at
            FROM hashtag_stats
            WHERE hashtag = ?
            ORDER BY collected_at DESC
            LIMIT 1
        """, (cleaned,)).fetchone()
        conn.close()
        return dict(row) if row else None
 
    except Exception as e:
        logger.error(f"Erro ao buscar stats mais recentes de #{cleaned}: {e}")
        return None
 
 
def get_stats_last_n_hours(hashtag: str, hours: int = 24) -> list[dict]:
    """
    Retorna coletas de uma hashtag nas últimas N horas.
 
    Usado pelo módulo de análise para calcular crescimento recente.
 
    Args:
        hashtag: Nome da hashtag (com ou sem #).
        hours:   Janela de tempo em horas (padrão: 24).
 
    Returns:
        Lista de dicts ordenada do mais antigo ao mais recente.
    """
    cleaned = normalize_hashtag(hashtag)
    since = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
 
    try:
        conn = get_connection()
        rows = conn.execute("""
            SELECT id, hashtag, views_total, videos_total,
                   avg_views, avg_likes, avg_comments, collected_at
            FROM hashtag_stats
            WHERE hashtag = ?
              AND collected_at >= ?
            ORDER BY collected_at ASC
        """, (cleaned, since)).fetchall()
        conn.close()
        return [dict(row) for row in rows]
 
    except Exception as e:
        logger.error(f"Erro ao buscar stats das últimas {hours}h de #{cleaned}: {e}")
        return []
 
 
# ─────────────────────────────────────────
#  4. IMPORTAÇÃO EM LOTE
# ─────────────────────────────────────────
 
def import_hashtags_from_list(hashtags: list[str]) -> dict:
    """
    Adiciona múltiplas hashtags de uma vez (ex: importação de arquivo).
 
    Args:
        hashtags: Lista de strings com nomes de hashtags.
 
    Returns:
        dict com:
            added   (int) — quantas foram adicionadas
            skipped (int) — quantas já existiam
            errors  (int) — quantas falharam por erro
    """
    results = {"added": 0, "skipped": 0, "errors": 0}
 
    for tag in hashtags:
        tag = tag.strip()
        if not tag:
            continue
        result = add_hashtag(tag)
        if result["success"]:
            results["added"] += 1
        elif "já está" in result["message"]:
            results["skipped"] += 1
        else:
            results["errors"] += 1
 
    logger.info(
        f"Importação concluída: "
        f"{results['added']} adicionadas, "
        f"{results['skipped']} ignoradas, "
        f"{results['errors']} erros."
    )
    return results