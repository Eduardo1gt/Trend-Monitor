# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  scraper/trends_scraper.py
#
#  Coleta dados de tendência via Google Trends (pytrends).
#
#  Mapeamento dos campos do banco:
#    views_total  -> indice de interesse atual (0-100)
#    videos_total -> None (Google Trends nao fornece)
#    avg_views    -> media do indice nos ultimos 30 dias
#    avg_likes    -> None
#    avg_comments -> None
#    collected_at -> timestamp da coleta
# ─────────────────────────────────────────────────────────────────────────────

import time
import random
from datetime import datetime
from typing import Optional

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError

from utils.helpers import (
    setup_logger,
    normalize_hashtag,
    get_request_timeout,
    get_request_delay,
)

logger = setup_logger(__name__)


# ─────────────────────────────────────────
#  CONSTANTES
# ─────────────────────────────────────────

TIMEFRAME_7D  = "now 7-d"   # ultimos 7 dias  (granularidade: horaria)
TIMEFRAME_30D = "today 1-m" # ultimos 30 dias (granularidade: diaria) — mais estavel
TIMEFRAME_90D = "today 3-m" # ultimos 90 dias (granularidade: semanal)

DEFAULT_GEO  = ""
DEFAULT_LANG = "pt-BR"

# Tipos de topico que o Google Trends reconhece como entidades confiaveis.
# Usados para validar se o MID encontrado e realmente o topico certo.
_TRUSTED_TOPIC_TYPES = {
    "video game", "game", "software", "programming language",
    "company", "person", "fictional character", "fiction",
    "fictional universe", "topic", "subject", "sport",
    "music genre", "anime", "animated series", "film",
    "tv show", "book", "card game", "esport",
}


# ─────────────────────────────────────────
#  1. CONEXAO COM O GOOGLE TRENDS
# ─────────────────────────────────────────

def _build_pytrends() -> TrendReq:
    """
    Cria e retorna uma instancia do cliente pytrends configurada
    com fuso horario brasileiro e retries automaticos.
    """
    timeout = get_request_timeout()
    return TrendReq(
        hl=DEFAULT_LANG,
        tz=180,                  # UTC-3 (Brasil)
        timeout=(timeout, timeout),
        retries=2,
        backoff_factor=0.5,
    )


# ─────────────────────────────────────────
#  2. BUSCA DE TOPICO (MID)
# ─────────────────────────────────────────

def _find_topic_mid(cleaned: str, pytrends: TrendReq) -> Optional[str]:
    """
    Busca o MID (identificador interno do Google) para um termo.

    Um MID identifica uma entidade especifica no grafo de conhecimento
    do Google — por exemplo, "/m/0abc" para "Genshin Impact (video game)".
    Usar o MID em vez do texto literal torna os dados muito mais estaveis,
    pois o Google nao mistura buscas nao relacionadas.

    Logica de match (do mais restrito ao mais permissivo):

        1. Match exato: cleaned == title
           Ex: "valorant" == "valorant" -> aceita

        2. Match de palavra completa: cleaned e uma palavra isolada no titulo
           Ex: "gaming" in ["gaming", "pc", "laptop"] -> aceita
               "lol"    in ["league", "of", "legends"] -> rejeita (nao esta)
               "lol"    in ["lol"] -> aceita (e a palavra inteira)

        3. Match de prefixo longo (>= 6 chars): cleaned e prefixo do titulo
           Ex: "leagueoflegends" startswith "league" -> aceita se len >= 6
               "lol" startswith "lollipop" -> rejeita (< 6 chars)

    Alem do match do titulo, valida o tipo do topico contra _TRUSTED_TOPIC_TYPES
    para evitar pegar entidades erradas (ex: empresa com nome similar ao jogo).

    Args:
        cleaned:  Termo normalizado (ex: "genshinimpact", "lol", "gaming").
        pytrends: Instancia do cliente pytrends.

    Returns:
        String com o MID (ex: "/m/0abc123") ou None se nao encontrar match seguro.
    """
    try:
        suggestions = pytrends.suggestions(cleaned)
        if not suggestions:
            return None

        for s in suggestions:
            title      = (s.get("title") or "").lower().strip()
            topic_type = (s.get("type")  or "").lower().strip()
            mid        = s.get("mid", "")

            if not mid or not title:
                continue

            # ── Valida o tipo do topico ──────────────────────────────────
            # Verifica se alguma palavra do tipo esta em _TRUSTED_TOPIC_TYPES
            # Ex: "action video game" -> "video game" esta no set -> valido
            type_words = topic_type.split()
            is_trusted = any(
                " ".join(type_words[i:i+2]) in _TRUSTED_TOPIC_TYPES
                or type_words[i] in _TRUSTED_TOPIC_TYPES
                for i in range(len(type_words))
            ) if type_words else False

            if not is_trusted:
                continue

            # ── Valida o match do titulo ─────────────────────────────────
            term_lower  = cleaned.lower()
            title_words = title.split()

            # Estrategia 1: match exato (mais seguro)
            if term_lower == title:
                logger.info(
                    f"MID encontrado (exato): '{s.get('title')}' "
                    f"({s.get('type')}) -> {mid}"
                )
                return mid

            # Estrategia 2: termo e uma palavra completa do titulo
            # Resolve "lol" != "lollipop": "lol" nao esta em ["lollipop"]
            # mas "lol" esta em ["lol", "esports"]
            if term_lower in title_words:
                logger.info(
                    f"MID encontrado (palavra completa): '{s.get('title')}' "
                    f"({s.get('type')}) -> {mid}"
                )
                return mid

            # Estrategia 3: termo e prefixo longo (>= 6 chars) do titulo
            # Resolve "leagueoflegends" -> titulo "league of legends"
            # sem titulo ter "leagueoflegends" como palavra exata
            # Rejeita "lol" (3 chars < 6) mesmo que titulo comece com "lol"
            if len(term_lower) >= 6 and title.replace(" ", "").startswith(term_lower):
                logger.info(
                    f"MID encontrado (prefixo longo): '{s.get('title')}' "
                    f"({s.get('type')}) -> {mid}"
                )
                return mid

        logger.debug(f"Nenhum MID confiavel encontrado para '{cleaned}'")
        return None

    except Exception as e:
        logger.warning(f"Falha ao buscar sugestoes para '{cleaned}': {e}")
        return None


# ─────────────────────────────────────────
#  3. COLETA DE INTERESSE AO LONGO DO TEMPO
# ─────────────────────────────────────────

def _fetch_interest(
    term: str,
    pytrends: TrendReq,
    timeframe: str = TIMEFRAME_30D,
    geo: str = DEFAULT_GEO,
) -> Optional[dict]:
    """
    Busca a serie temporal de interesse para um termo ou MID.

    Args:
        term:      Termo ou MID (ex: "gaming" ou "/m/0abc").
        pytrends:  Instancia do cliente pytrends.
        timeframe: Janela de tempo (padrao: 30 dias para maior estabilidade).
        geo:       Regiao geografica (padrao: mundial).

    Returns:
        Dict com:
            current (int)   — indice mais recente (0-100)
            avg_7d  (float) — media do periodo
            peak    (int)   — maior valor do periodo
            low     (int)   — menor valor do periodo
            series  (list)  — serie temporal completa
        Ou None se falhar.
    """
    try:
        logger.debug(f"Buscando interesse para '{term}' ({timeframe})")

        pytrends.build_payload(
            kw_list=[term],
            timeframe=timeframe,
            geo=geo,
        )

        df = pytrends.interest_over_time()

        if df is None or df.empty:
            logger.debug(f"Sem dados para '{term}'")
            return None

        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        series = df[term].tolist()

        if not series:
            return None

        return {
            "current": int(series[-1]),
            "avg_7d":  round(sum(series) / len(series), 2),
            "peak":    int(max(series)),
            "low":     int(min(series)),
            "series":  series,
        }

    except ResponseError as e:
        logger.warning(f"Google Trends bloqueou requisicao para '{term}': {e}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar interesse para '{term}': {type(e).__name__}: {e}")
        return None


def _fetch_related_rising(
    term: str,
    pytrends: TrendReq,
) -> Optional[list[dict]]:
    """
    Busca termos relacionados que estao crescendo rapidamente.
    Requer que build_payload ja tenha sido chamado para o termo.

    Returns:
        Lista de dicts {"term": str, "growth": str} ou None.
        "Breakout" = crescimento > 5000%.
    """
    try:
        related   = pytrends.related_queries()
        rising_df = related.get(term, {}).get("rising")

        if rising_df is None or rising_df.empty:
            return None

        return [
            {"term": row["query"], "growth": str(row["value"])}
            for _, row in rising_df.head(5).iterrows()
        ]

    except Exception as e:
        logger.debug(f"Erro ao buscar termos relacionados para '{term}': {e}")
        return None


# ─────────────────────────────────────────
#  4. DADOS SIMULADOS (FALLBACK)
# ─────────────────────────────────────────

_SIMULATED_BASES = {
    "gaming":          88,
    "genshinimpact":   72,
    "valorant":        65,
    "leagueoflegends": 61,
    "lol":             58,
    "minecraft":       80,
    "fortnite":        75,
    "wutheringwaves":  55,
    "anime":           78,
    "music":           90,
    "dance":           70,
    "_default":        40,
}


def _fetch_simulated(term: str) -> dict:
    """
    Gera dados simulados realistas quando o Google Trends falha.
    Nunca retorna None — garante que o sistema nunca quebra.
    """
    logger.warning(
        f"[Simulado] Usando dados simulados para '{term}'. "
        f"Google Trends falhou (rate limit ou sem conexao)."
    )

    base   = _SIMULATED_BASES.get(term, _SIMULATED_BASES["_default"])
    series = []
    value  = base * random.uniform(0.85, 1.0)

    for _ in range(7):
        value = max(0, min(100, value + random.uniform(-8, 10)))
        series.append(int(value))

    current = series[-1]
    avg_7d  = round(sum(series) / len(series), 2)

    return {
        "hashtag":         term,
        "views_total":     current,
        "videos_total":    None,
        "avg_views":       avg_7d,
        "avg_likes":       None,
        "avg_comments":    None,
        "collected_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source":          "simulated",
        "source_id":       term,
        "interest_series": series,
        "peak":            max(series),
        "low":             min(series),
        "related_rising":  None,
    }


# ─────────────────────────────────────────
#  5. FUNCAO PRINCIPAL DE COLETA
# ─────────────────────────────────────────

def collect_term(term: str, geo: str = DEFAULT_GEO) -> dict:
    """
    Coleta dados de tendencia de um termo via Google Trends.

    Pipeline completo com 4 estagios em cascata:

        Estagio 1 — Busca MID (identificador de topico do Google)
            Usa _find_topic_mid() com match inteligente para evitar
            falsos positivos como "lol" -> "lollipop".

        Estagio 2 — Coleta com MID (mais estavel)
            Se encontrou MID, usa ele para buscar dados.
            Dados de topico sao mais estaveis pois o Google
            nao mistura buscas nao relacionadas.

        Estagio 3 — Fallback para termo literal
            Se o MID retornou vazio (topico sem dados no periodo),
            tenta o texto puro como busca de termo.

        Estagio 4 — Fallback para dados simulados
            Se tudo falhar (rate limit, sem conexao), usa dados
            simulados realistas. O sistema nunca quebra.

    Args:
        term: Termo de busca (com ou sem #).
        geo:  Codigo ISO do pais ("" = mundial, "BR" = Brasil).

    Returns:
        Dict compativel com database_manager.save_stats():
        {
            hashtag, views_total, videos_total, avg_views,
            avg_likes, avg_comments, collected_at,
            source, source_id, interest_series, peak, low, related_rising
        }
    """
    cleaned  = normalize_hashtag(term)
    logger.info(f"Coletando '{cleaned}' (geo='{geo or 'mundial'}')")

    pytrends       = _build_pytrends()
    search_keyword = cleaned
    is_topic       = False

    # ── Estagio 1: busca MID ────────────────────────────────────────────────
    mid = _find_topic_mid(cleaned, pytrends)
    if mid:
        search_keyword = mid
        is_topic       = True

    # ── Estagio 2: coleta com MID ou termo ──────────────────────────────────
    interest = _fetch_interest(search_keyword, pytrends, timeframe=TIMEFRAME_30D, geo=geo)

    # ── Estagio 3: fallback para termo literal se MID falhou ─────────────────
    if not interest and is_topic:
        logger.warning(
            f"MID '{search_keyword}' retornou vazio. "
            f"Fallback para termo literal '{cleaned}'."
        )
        search_keyword = cleaned
        is_topic       = False
        interest       = _fetch_interest(cleaned, pytrends, timeframe=TIMEFRAME_30D, geo=geo)

    # ── Estagio 4: fallback simulado ─────────────────────────────────────────
    if not interest:
        return _fetch_simulated(cleaned)

    # Termos relacionados — tenta com o keyword usado, depois com o literal
    related = _fetch_related_rising(search_keyword, pytrends)
    if not related and is_topic:
        related = _fetch_related_rising(cleaned, pytrends)

    source = "google_trends_topic" if is_topic else "google_trends_term"
    logger.info(
        f"✓ '{cleaned}' [{source}] -> "
        f"indice: {interest['current']} | "
        f"media: {interest['avg_7d']} | "
        f"pico: {interest['peak']}"
    )

    return {
        "hashtag":         cleaned,
        "views_total":     interest["current"],
        "videos_total":    None,
        "avg_views":       interest["avg_7d"],
        "avg_likes":       None,
        "avg_comments":    None,
        "collected_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source":          source,
        "source_id":       search_keyword,
        "interest_series": interest["series"],
        "peak":            interest["peak"],
        "low":             interest["low"],
        "related_rising":  related,
    }


# ─────────────────────────────────────────
#  6. COLETA EM LOTE
# ─────────────────────────────────────────

def collect_multiple_terms(
    terms: list[str],
    geo: str = DEFAULT_GEO,
) -> list[dict]:
    """
    Coleta dados de uma lista de termos com delay entre cada um.
    O delay evita rate limiting do Google Trends.
    """
    results = []
    delay   = get_request_delay()
    total   = len(terms)

    for i, term in enumerate(terms, 1):
        cleaned = normalize_hashtag(term)
        logger.info(f"Coletando {i}/{total}: '{cleaned}'")

        result = collect_term(term, geo=geo)
        results.append(result)

        if i < total:
            sleep_time = delay + random.uniform(delay * 0.5, delay)
            logger.debug(f"Aguardando {sleep_time:.1f}s antes do proximo termo...")
            time.sleep(sleep_time)

    logger.info(f"Coleta concluida: {total} termos processados.")
    return results


# ─────────────────────────────────────────
#  7. TRENDING EM TEMPO REAL
# ─────────────────────────────────────────

def get_trending_now(geo: str = "BR") -> list[dict]:
    """
    Retorna os termos que estao em alta AGORA no Google Trends.
    Util para sugerir novas hashtags para monitorar.
    """
    try:
        logger.info(f"Buscando trending now para geo='{geo}'")
        pytrends = _build_pytrends()
        df       = pytrends.trending_searches(pn=_geo_to_country_name(geo))

        if df is None or df.empty:
            return []

        trending = [
            {"term": str(row[0]), "traffic": "trending"}
            for _, row in df.head(20).iterrows()
        ]
        logger.info(f"✓ {len(trending)} trending topics para '{geo}'")
        return trending

    except Exception as e:
        logger.error(f"Erro ao buscar trending now: {e}")
        return []


def _geo_to_country_name(geo: str) -> str:
    """Converte codigo ISO para nome de pais que o pytrends aceita."""
    mapping = {
        "BR": "brazil",
        "US": "united_states",
        "GB": "united_kingdom",
        "JP": "japan",
        "KR": "south_korea",
        "FR": "france",
        "DE": "germany",
        "MX": "mexico",
        "AR": "argentina",
        "":   "brazil",
    }
    return mapping.get(geo.upper(), "brazil")