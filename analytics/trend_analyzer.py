# -*- coding: utf-8 -*-
# ─────────────────────────────────────────────────────────────────────────────
#  analytics/trend_analyzer.py
#
#  Transforma dados brutos do banco em métricas de tendência:
#    - Crescimento percentual (24h, 7d)
#    - Média móvel
#    - Velocidade e direção da tendência
#    - Detecção de viralização
#    - Ranking de hashtags por popularidade
# ─────────────────────────────────────────────────────────────────────────────

from typing import Optional

from database.database_manager import (
    get_stats_history,
    get_stats_last_n_hours,
    get_latest_stats,
    get_all_hashtags,
)
from utils.helpers import setup_logger, format_percentage

logger = setup_logger(__name__)


# ─────────────────────────────────────────
#  CONSTANTES
# ─────────────────────────────────────────

# Limiar de crescimento para considerar que algo está viralizando
VIRAL_THRESHOLD_24H  = 50.0   # +50% em 24h  = viralizando
VIRAL_THRESHOLD_7D   = 200.0  # +200% em 7d  = tendência forte

# Limiar para considerar crescimento acelerado
ACCELERATING_THRESHOLD = 10.0  # média móvel subindo >10 pontos

# Janela da média móvel (em número de coletas)
MOVING_AVG_WINDOW = 3


# ─────────────────────────────────────────
#  1. CRESCIMENTO PERCENTUAL
# ─────────────────────────────────────────

def calculate_growth(old_value: float, new_value: float) -> Optional[float]:
    """
    Calcula o crescimento percentual entre dois valores.

    Fórmula: ((novo - antigo) / antigo) * 100

    Exemplos:
        old=72, new=87  →  +20.83%
        old=87, new=72  →  -17.24%
        old=0,  new=50  →  None  (divisão por zero)

    Args:
        old_value: Valor anterior.
        new_value: Valor mais recente.

    Returns:
        Float com o percentual (positivo = crescimento, negativo = queda)
        ou None se o valor antigo for zero.
    """
    if old_value is None or new_value is None:
        return None
    if old_value == 0:
        return None
    return round(((new_value - old_value) / old_value) * 100, 2)


def get_growth_24h(term: str) -> Optional[float]:
    """
    Calcula o crescimento do índice de interesse nas últimas 24 horas.

    Compara a coleta mais antiga disponível nas últimas 24h
    com a coleta mais recente.

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        Float com o percentual de crescimento ou None se não houver
        dados suficientes (menos de 2 coletas nas últimas 24h).
    """
    records = get_stats_last_n_hours(term, hours=24)

    if len(records) < 2:
        logger.debug(f"Dados insuficientes para crescimento 24h de '{term}'")
        return None

    old_value = records[0].get("views_total")
    new_value = records[-1].get("views_total")

    growth = calculate_growth(old_value, new_value)
    logger.debug(f"Crescimento 24h de '{term}': {format_percentage(growth)}")
    return growth


def get_growth_7d(term: str) -> Optional[float]:
    """
    Calcula o crescimento do índice de interesse nos últimos 7 dias.

    Usa as últimas 168 horas (7 * 24) de histórico.

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        Float com o percentual de crescimento ou None se insuficiente.
    """
    records = get_stats_last_n_hours(term, hours=168)

    if len(records) < 2:
        return None

    old_value = records[0].get("views_total")
    new_value = records[-1].get("views_total")

    return calculate_growth(old_value, new_value)


# ─────────────────────────────────────────
#  2. MÉDIA MÓVEL
# ─────────────────────────────────────────

def calculate_moving_average(
    values: list[float],
    window: int = MOVING_AVG_WINDOW,
) -> list[float]:
    """
    Calcula a média móvel simples de uma série de valores.

    A média móvel suaviza variações bruscas e revela a tendência
    real por trás dos dados — sem os picos e vales temporários.

    Como funciona com window=3:
        valores:       [72, 68, 75, 81, 79, 85, 87]
        média móvel:   [70.0, 71.5, 71.7, 74.7, 78.3, 81.7, 83.7]
                        ↑ primeiros pontos usam janela menor
                          para não perder dados no início da série

    Args:
        values: Lista de valores numéricos (série temporal).
        window: Tamanho da janela (padrão: 3 coletas).

    Returns:
        Lista de floats com a média móvel, mesma length da entrada.
    """
    if not values:
        return []

    result = []
    for i in range(len(values)):
        # Usa janela menor no início para não perder os primeiros pontos
        start = max(0, i - window + 1)
        window_values = values[start : i + 1]
        avg = sum(window_values) / len(window_values)
        result.append(round(avg, 2))

    return result


def get_moving_average_series(term: str, limit: int = 30) -> dict:
    """
    Retorna a série temporal e sua média móvel para um termo.

    Usado pelos gráficos do dashboard para plotar a linha suavizada
    junto com os valores reais.

    Args:
        term:  Nome do termo (com ou sem #).
        limit: Número máximo de registros históricos (padrão: 30).

    Returns:
        Dict com:
            dates:          list[str]   — timestamps das coletas
            values:         list[float] — índices reais
            moving_average: list[float] — série suavizada
        Retorna dicts com listas vazias se não houver dados.
    """
    records = get_stats_history(term, limit=limit)

    if not records:
        return {"dates": [], "values": [], "moving_average": []}

    # Histórico vem do mais recente ao mais antigo — inverte para cronológico
    records = list(reversed(records))

    dates  = [r["collected_at"] for r in records]
    values = [r["views_total"] or 0 for r in records]
    moving_avg = calculate_moving_average(values)

    return {
        "dates":          dates,
        "values":         values,
        "moving_average": moving_avg,
    }


# ─────────────────────────────────────────
#  3. VELOCIDADE E DIREÇÃO DA TENDÊNCIA
# ─────────────────────────────────────────

def get_trend_direction(term: str) -> str:
    """
    Determina a direção atual da tendência de um termo.

    Compara a média das últimas 3 coletas com as 3 anteriores.
    Isso é mais robusto do que comparar apenas 2 pontos isolados,
    pois ignora picos e vales temporários.

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        Uma das strings:
            "subindo"   — tendência de alta
            "caindo"    — tendência de baixa
            "estável"   — sem variação significativa
            "indefinido"— dados insuficientes
    """
    records = get_stats_history(term, limit=6)

    if len(records) < 4:
        return "indefinido"

    # Records vêm do mais recente — divide em duas metades
    recent = records[:3]   # 3 mais recentes
    older  = records[3:6]  # 3 anteriores

    recent_avg = sum(r["views_total"] or 0 for r in recent) / len(recent)
    older_avg  = sum(r["views_total"] or 0 for r in older)  / len(older)

    diff = recent_avg - older_avg

    if diff > 3:      # subiu mais de 3 pontos no índice
        return "subindo"
    elif diff < -3:   # caiu mais de 3 pontos
        return "caindo"
    else:
        return "estável"


def get_trend_velocity(term: str) -> str:
    """
    Determina a VELOCIDADE da mudança de tendência.

    Compara o crescimento recente (últimas 24h) com o crescimento
    do período anterior (24h–48h atrás) para saber se está
    acelerando ou desacelerando.

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        Uma das strings:
            "acelerando"    — crescimento está aumentando
            "desacelerando" — crescimento está diminuindo
            "estável"       — velocidade constante
            "indefinido"    — dados insuficientes
    """
    records_48h = get_stats_last_n_hours(term, hours=48)

    if len(records_48h) < 3:
        return "indefinido"

    midpoint = len(records_48h) // 2

    # Divide em duas janelas: mais recente e anterior
    recent_half = records_48h[midpoint:]
    older_half  = records_48h[:midpoint]

    if not recent_half or not older_half:
        return "indefinido"

    growth_recent = calculate_growth(
        older_half[0].get("views_total"),
        recent_half[-1].get("views_total"),
    )
    growth_older = calculate_growth(
        older_half[0].get("views_total"),
        older_half[-1].get("views_total"),
    )

    if growth_recent is None or growth_older is None:
        return "indefinido"

    diff = growth_recent - growth_older

    if diff > ACCELERATING_THRESHOLD:
        return "acelerando"
    elif diff < -ACCELERATING_THRESHOLD:
        return "desacelerando"
    else:
        return "estável"


# ─────────────────────────────────────────
#  4. DETECÇÃO DE VIRALIZAÇÃO
# ─────────────────────────────────────────

def is_viral(term: str) -> bool:
    """
    Detecta se um termo está viralizando com base nos limiares definidos.

    Um termo é considerado viral se:
        - Cresceu mais de 50% nas últimas 24h, OU
        - Cresceu mais de 200% nos últimos 7 dias

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        True se viral, False caso contrário.
    """
    growth_24h = get_growth_24h(term)
    growth_7d  = get_growth_7d(term)

    if growth_24h is not None and growth_24h >= VIRAL_THRESHOLD_24H:
        logger.info(f"🔥 '{term}' está VIRAL! Crescimento 24h: +{growth_24h}%")
        return True

    if growth_7d is not None and growth_7d >= VIRAL_THRESHOLD_7D:
        logger.info(f"🔥 '{term}' está VIRAL! Crescimento 7d: +{growth_7d}%")
        return True

    return False


def get_trend_status(term: str) -> dict:
    """
    Retorna um resumo completo do status de tendência de um termo.

    É a função principal que o dashboard chama para montar
    o card de resumo de cada hashtag/termo monitorado.

    Args:
        term: Nome do termo (com ou sem #).

    Returns:
        Dict completo com todas as métricas:
        {
            "term":        str,          — nome normalizado
            "current":     int | None,   — índice atual (0–100)
            "growth_24h":  float | None, — crescimento % em 24h
            "growth_7d":   float | None, — crescimento % em 7d
            "direction":   str,          — "subindo"|"caindo"|"estável"
            "velocity":    str,          — "acelerando"|"desacelerando"
            "is_viral":    bool,         — True se viralizando
            "status_label":str,          — emoji + texto para o dashboard
            "last_updated":str | None,   — timestamp da última coleta
        }
    """
    latest    = get_latest_stats(term)
    growth_24h = get_growth_24h(term)
    growth_7d  = get_growth_7d(term)
    direction  = get_trend_direction(term)
    velocity   = get_trend_velocity(term)
    viral      = is_viral(term)

    current     = latest.get("views_total") if latest else None
    last_updated = latest.get("collected_at") if latest else None

    # Monta label de status para exibir no dashboard
    status_label = _build_status_label(viral, direction, velocity)

    return {
        "term":         term,
        "current":      current,
        "growth_24h":   growth_24h,
        "growth_7d":    growth_7d,
        "direction":    direction,
        "velocity":     velocity,
        "is_viral":     viral,
        "status_label": status_label,
        "last_updated": last_updated,
    }


def _build_status_label(viral: bool, direction: str, velocity: str) -> str:
    """
    Constrói um label de status legível para exibir no dashboard.

    Args:
        viral:     Se o termo está viralizando.
        direction: Direção da tendência.
        velocity:  Velocidade da mudança.

    Returns:
        String formatada com emoji para o dashboard.
        Exemplos:
            "🔥 Viralizando"
            "📈 Subindo · Acelerando"
            "📉 Caindo · Desacelerando"
            "➡️ Estável"
    """
    if viral:
        return "🔥 Viralizando"

    dir_map = {
        "subindo":    "📈 Subindo",
        "caindo":     "📉 Caindo",
        "estável":    "➡️ Estável",
        "indefinido": "❓ Indefinido",
    }
    vel_map = {
        "acelerando":    "Acelerando",
        "desacelerando": "Desacelerando",
        "estável":       "",
        "indefinido":    "",
    }

    dir_label = dir_map.get(direction, "❓ Indefinido")
    vel_label = vel_map.get(velocity, "")

    if vel_label:
        return f"{dir_label} · {vel_label}"
    return dir_label


# ─────────────────────────────────────────
#  5. RANKING DE TERMOS
# ─────────────────────────────────────────

def get_trending_ranking() -> list[dict]:
    """
    Retorna todas as hashtags monitoradas ordenadas por popularidade atual.

    Usado pelo dashboard para mostrar o ranking geral e destacar
    os termos que mais estão crescendo.

    Returns:
        Lista de dicts ordenada pelo índice atual (maior primeiro).
        Cada dict contém: term, current, growth_24h, growth_7d,
        direction, velocity, is_viral, status_label, last_updated.

        Exemplo de saída:
        [
            {"term": "wutheringwaves", "current": 95, "growth_24h": 340.0,
             "is_viral": True,  "status_label": "🔥 Viralizando"},
            {"term": "genshinimpact", "current": 72, "growth_24h": 210.0,
             "is_viral": True,  "status_label": "🔥 Viralizando"},
            {"term": "valorant",      "current": 61, "growth_24h": 4.2,
             "is_viral": False, "status_label": "📈 Subindo · Acelerando"},
        ]
    """
    hashtags = get_all_hashtags()

    if not hashtags:
        return []

    ranking = []
    for entry in hashtags:
        term   = entry["hashtag"]
        status = get_trend_status(term)
        ranking.append(status)

    # Ordena: virais primeiro, depois pelo índice atual (maior → menor)
    ranking.sort(
        key=lambda x: (
            not x["is_viral"],              # virais no topo (False < True)
            -(x["current"] or 0),           # maior índice primeiro
        )
    )

    return ranking


def get_viral_alerts() -> list[dict]:
    """
    Retorna apenas os termos que estão viralizando agora.

    Usado para exibir o painel de alertas no dashboard —
    o equivalente ao output do exemplo do projeto:

        Trending topics detected:
        #WutheringWaves  ↑ 340%
        #GenshinLeaks    ↑ 210%

    Returns:
        Lista de dicts dos termos virais com crescimento formatado.
        Lista vazia se nenhum termo estiver viralizando.
    """
    ranking = get_trending_ranking()
    alerts  = [r for r in ranking if r["is_viral"]]

    if alerts:
        logger.info(f"🚨 {len(alerts)} termo(s) viralizando agora!")
        for alert in alerts:
            logger.info(
                f"  #{alert['term']} "
                f"↑ {format_percentage(alert['growth_24h'])} (24h)"
            )

    return alerts