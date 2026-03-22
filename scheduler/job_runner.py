# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  scheduler/job_runner.py
#
#  Orquestra o pipeline completo de coleta e análise:
#    1. Busca todas as hashtags monitoradas no banco
#    2. Dispara o scraper para cada uma
#    3. Salva os resultados no banco
#    4. Roda o analyzer para detectar virais
#    5. Agenda repetição automática se configurado no .env
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime
from typing import Callable, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from database.database_manager import get_all_hashtags, save_stats
from scraper.trends_scraper import collect_multiple_terms
from analytics.trend_analyzer import get_viral_alerts
from utils.helpers import (
    setup_logger,
    get_collection_interval,
    format_percentage,
)

logger = setup_logger(__name__)

# Instância global do scheduler — criada uma vez, reutilizada
_scheduler: Optional[BackgroundScheduler] = None


# ─────────────────────────────────────────
#  1. PIPELINE DE COLETA
# ─────────────────────────────────────────

def run_collection(on_complete: Optional[Callable] = None, geo: str = "") -> dict:
    """
    Executa o pipeline completo de coleta para todos os termos monitorados.

    Fluxo:
        1. Busca lista de hashtags no banco
        2. Coleta dados via Google Trends para cada uma
        3. Salva cada resultado no banco
        4. Detecta virais e loga alertas
        5. Chama callback on_complete se fornecido (usado pelo dashboard)

    Args:
        on_complete: Funcao opcional chamada ao final com o resultado.
        geo:         Regiao para coleta ("" = global, "BR" = Brasil, etc.)
                     Global e o padrao — muito mais estavel que paises individuais.

    Returns:
        Dict com o resumo da execucao:
        {
            "started_at":  str,   — timestamp de inicio
            "finished_at": str,   — timestamp de fim
            "total":       int,   — total de termos processados
            "success":     int,   — quantos salvaram com sucesso
            "failed":      int,   — quantos falharam ao salvar
            "viral_count": int,   — quantos estão viralizando
            "virals":      list,  — lista dos termos virais
        }
    """
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 55)
    logger.info("🚀 Iniciando pipeline de coleta...")
    logger.info(f"   Início: {started_at}")

    # busca hashtags cadastradas
    hashtags = get_all_hashtags()

    if not hashtags:
        logger.warning("Nenhuma hashtag cadastrada. Adicione termos para monitorar.")
        result = {
            "started_at":  started_at,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total":       0,
            "success":     0,
            "failed":      0,
            "viral_count": 0,
            "virals":      [],
        }
        if on_complete:
            on_complete(result)
        return result

    terms = [h["hashtag"] for h in hashtags]
    logger.info(f"   Termos para coletar: {len(terms)}")

    # coleta via Google Trends para todos os termos (pode ser lento, dependendo da quantidade)
    collected = collect_multiple_terms(terms, geo=geo)

    # salva cada resultado no banco
    success = 0
    failed  = 0

    for data in collected:
        term   = data.get("hashtag", "?")
        source = data.get("source", "?")

        # Remove campos extras que o banco não conhece
        # (interest_series, peak, low, related_rising, source)
        stats_to_save = {
            "hashtag":      data.get("hashtag"),
            "views_total":  data.get("views_total"),
            "videos_total": data.get("videos_total"),
            "avg_views":    data.get("avg_views"),
            "avg_likes":    data.get("avg_likes"),
            "avg_comments": data.get("avg_comments"),
            "collected_at": data.get("collected_at"),
        }

        saved = save_stats(stats_to_save)

        if saved:
            success += 1
            logger.info(
                f"   ✓ #{term} salvo "
                f"(índice: {data.get('views_total')} | fonte: {source})"
            )
        else:
            failed += 1
            logger.error(f"   ✗ #{term} falhou ao salvar no banco")

    # detecta virais 
    virals      = get_viral_alerts()
    viral_count = len(virals)

    if viral_count > 0:
        logger.info(f"🚨 {viral_count} termo(s) viralizando agora:")
        for v in virals:
            growth_str = format_percentage(v.get("growth_24h"))
            logger.info(f"   #{v['term']} ↑ {growth_str} (24h)")
    else:
        logger.info("   Nenhum viral detectado nesta coleta.")


    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"✅ Pipeline concluído em {finished_at}")
    logger.info(
        f"   Resultado: {success} salvos | "
        f"{failed} falhas | "
        f"{viral_count} virais"
    )
    logger.info("=" * 55)

    result = {
        "started_at":  started_at,
        "finished_at": finished_at,
        "total":       len(terms),
        "success":     success,
        "failed":      failed,
        "viral_count": viral_count,
        "virals":      virals,
    }

    #  notifica dashboard se callback fornecido
    if on_complete:
        try:
            on_complete(result)
        except Exception as e:
            logger.error(f"Erro no callback on_complete: {e}")

    return result


# ─────────────────────────────────────────
#  2. AGENDAMENTO AUTOMÁTICO
# ─────────────────────────────────────────

def start_scheduler(on_complete: Optional[Callable] = None) -> bool:
    """
    Inicia o agendamento automático de coleta se configurado no .env.

    Lê COLLECTION_INTERVAL_HOURS do .env:
        0 → não agenda (coleta só manual, ao executar o programa)
        N → agenda coleta a cada N horas em background

    O scheduler roda em uma thread separada (BackgroundScheduler),
    então não bloqueia a interface do Streamlit.

    Args:
        on_complete: Callback repassado para run_collection a cada execução.

    Returns:
        True se o scheduler foi iniciado, False se modo manual (intervalo=0).
    """
    global _scheduler

    interval_hours = get_collection_interval()

    if interval_hours == 0:
        logger.info(
            "⏱️  Modo manual ativo — coleta ocorre apenas ao executar o programa. "
            "(Para agendar automaticamente, defina COLLECTION_INTERVAL_HOURS no .env)"
        )
        return False

    if _scheduler and _scheduler.running:
        logger.warning("Scheduler já está rodando. Ignorando nova inicialização.")
        return True

    _scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")

    _scheduler.add_job(
        func=lambda: run_collection(on_complete),
        trigger=IntervalTrigger(hours=interval_hours),
        id="coleta_automatica",
        name=f"Coleta automática a cada {interval_hours}h",
        replace_existing=True,
        max_instances=1,
    )

    _scheduler.start()

    logger.info(
        f"📅 Scheduler iniciado — coleta automática "
        f"a cada {interval_hours} hora(s)."
    )
    return True


def stop_scheduler() -> None:
    """
    Para o scheduler se estiver rodando.

    Chamado quando o programa encerra para liberar recursos
    e evitar threads órfãs.
    """
    global _scheduler

    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("🛑 Scheduler encerrado.")
    else:
        logger.debug("Scheduler não estava rodando.")


def get_scheduler_status() -> dict:
    """
    Retorna o status atual do scheduler.

    Usado pelo dashboard para exibir informações sobre
    o agendamento na interface.

    Returns:
        Dict com:
        {
            "running":          bool,        — se está ativo
            "interval_hours":   int,         — intervalo configurado
            "mode":             str,         — "automático" | "manual"
            "next_run":         str | None,  — próxima execução agendada
        }
    """
    interval_hours = get_collection_interval()
    running        = bool(_scheduler and _scheduler.running)
    next_run       = None

    if running:
        jobs = _scheduler.get_jobs()
        if jobs:
            next_run_dt = jobs[0].next_run_time
            if next_run_dt:
                next_run = next_run_dt.strftime("%d/%m/%Y às %H:%M")

    return {
        "running":        running,
        "interval_hours": interval_hours,
        "mode":           "automático" if interval_hours > 0 else "manual",
        "next_run":       next_run,
    }


# ─────────────────────────────────────────
#  3. INICIALIZAÇÃO COMPLETA
# ─────────────────────────────────────────

def initialize(on_complete: Optional[Callable] = None) -> None:
    """
    Ponto de entrada principal chamado pelo main.py na inicialização.

    Executa:
        1. Uma coleta imediata ao iniciar o programa
        2. Inicia o scheduler se COLLECTION_INTERVAL_HOURS > 0

    Args:
        on_complete: Callback opcional para notificar o dashboard
                     após cada coleta (imediata e agendadas).
    """
    logger.info("Inicializando job runner...")

    # Coleta imediata ao iniciar — independente do modo
    logger.info("Executando coleta inicial...")
    run_collection(on_complete)

    # Inicia agendamento se configurado
    start_scheduler(on_complete)