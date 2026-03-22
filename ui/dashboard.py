# -*- coding: utf-8 -*-
# ui/dashboard.py

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

from database.database_manager import (
    get_all_hashtags,
    add_hashtag,
    remove_hashtag,
    get_latest_stats,
    get_stats_history,
    get_connection,
    import_hashtags_from_list,
)
from analytics.trend_analyzer import (
    get_trend_status,
    get_trending_ranking,
    get_viral_alerts,
    get_moving_average_series,
)
from scraper.trends_scraper import collect_term
from scheduler.job_runner import run_collection, get_scheduler_status
from utils.helpers import (
    setup_logger,
    normalize_hashtag,
    format_hashtag_display,
    format_number,
    format_percentage,
    format_datetime_display,
    is_valid_hashtag,
)

logger = setup_logger(__name__)

CHART_COLORS = [
    "#7c85f5", "#f5a623", "#4caf50", "#e91e63",
    "#00bcd4", "#ff5722", "#9c27b0", "#ffeb3b",
]

def _get_color(index: int) -> str:
    return CHART_COLORS[index % len(CHART_COLORS)]


def _to_datetime(dt_str: str) -> datetime:
    """
    Converte string do banco para objeto datetime para ordenacao.
    SEMPRE usa o timestamp completo — nunca o valor formatado — para ordenar.
    """
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.min


def _fmt_date(dt_str: str) -> str:
    """
    Formata timestamp do banco para exibicao no eixo X dos graficos.
    Chamada SEMPRE DEPOIS de ordenar — nunca para ordenar.
    Ex: "2026-03-14 18:24:04" -> "14/03 18:24"
    """
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m %H:%M")
    except Exception:
        return dt_str


def _sort_records_chronological(records: list[dict]) -> list[dict]:
    """
    Ordena registros em ordem cronologica (mais antigo primeiro).
    Usa datetime real para garantir ordem correta entre dias diferentes.
    """
    return sorted(records, key=lambda r: _to_datetime(r["collected_at"]))


def _get_avg_index(term: str, limit: int = 50) -> "float | None":
    """Media do indice de todas as coletas. Mais estavel que o valor instantaneo."""
    records = get_stats_history(term, limit=limit)
    values  = [r["views_total"] for r in records if r["views_total"] is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 1)


def _calculate_moving_average(values: list[float], window: int = 3) -> list[float]:
    """Media movel simples — suaviza oscilacoes do indice do Google Trends."""
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start:i+1]
        result.append(round(sum(chunk) / len(chunk), 1))
    return result


# ─────────────────────────────────────────
#  TERMOS RELACIONADOS
# ─────────────────────────────────────────

def _get_related_rising(term: str) -> list[dict] | None:
    """
    Busca termos relacionados em alta (global).
    Usa cache de session_state para nao repetir a requisicao a cada rerender.
    """
    cache_key = f"related_{term}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]
    try:
        result  = collect_term(term, geo="")
        related = result.get("related_rising")
        st.session_state[cache_key] = related
        return related
    except Exception:
        return None


# ─────────────────────────────────────────
#  1. SIDEBAR
# ─────────────────────────────────────────

def render_sidebar() -> str | None:
    with st.sidebar:

        # Logo + titulo
        col_logo, col_title = st.columns([1, 4])
        with col_logo:
            try:
                st.image("assets/Fire.png", width=40)
            except Exception:
                st.markdown("<h2 style='margin-top: 0px;'>🔥</h2>", unsafe_allow_html=True)
        with col_title:
            st.markdown(
                """
                <div style="
                    font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
                    font-size: 26px;
                    font-weight: 800;
                    letter-spacing: -1.2px;
                    color: var(--text-color);
                    margin-top: 6px;
                ">
                    Trend<span style="font-weight: 300; color: #d93838;">Monitor</span>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.divider()

        # Adicionar hashtag
        st.subheader("➕ Adicionar Termo")
        col1, col2 = st.columns([3, 1])
        with col1:
            new_term = st.text_input(
                label="Termo",
                placeholder="ex: gaming",
                label_visibility="collapsed",
                key="input_new_term",
            )
        with col2:
            if st.button("Add", use_container_width=True) and new_term:
                _handle_add_hashtag(new_term)

        with st.expander("📂 Importar lista"):
            uploaded = st.file_uploader(
                "Arquivo .txt (uma hashtag por linha)",
                type=["txt"],
                key="file_uploader",
            )
            if uploaded:
                _handle_import_file(uploaded)

        st.divider()

        # Monitorando + botao Comparar
        hashtags = get_all_hashtags()
        selected = None

        col_title2, col_cmp = st.columns([3, 2])
        with col_title2:
            st.subheader("📋 Monitorando")
        with col_cmp:
            st.write("")
            if hashtags and st.button(
                "Comparar",
                use_container_width=True,
                key="btn_open_compare",
            ):
                st.session_state.show_compare  = True
                st.session_state.selected_term = None
                st.rerun()

        if not hashtags:
            st.caption("Nenhum termo cadastrado ainda.")
        else:
            if "selected_term" not in st.session_state:
                st.session_state.selected_term = None

            for i, entry in enumerate(hashtags):
                term  = entry["hashtag"]
                idx   = _get_avg_index(term)
                color = _get_color(i)
                label = format_hashtag_display(term)
                if idx is not None:
                    label += f"  `{idx}`"

                is_selected      = st.session_state.selected_term == term
                col_btn, col_del = st.columns([4, 1])

                with col_btn:
                    if st.button(
                        label,
                        key=f"btn_{term}",
                        use_container_width=True,
                        type="primary" if is_selected else "secondary",
                    ):
                        st.session_state.selected_term = None if is_selected else term
                        st.session_state.show_compare  = False
                        st.rerun()

                with col_del:
                    if st.button("✕", key=f"del_{term}"):
                        _handle_remove_hashtag(term)

            selected = st.session_state.get("selected_term")

        st.divider()

        # Coleta (sempre global — mais estavel)
        st.subheader("⚡ Coleta")
        if st.button("▶ Coletar Agora", use_container_width=True, type="primary"):
            _handle_run_collection()

        status = get_scheduler_status()
        modo   = "🕐 Automatico" if status["mode"] == "automatico" else "🖐 Manual"
        st.caption(f"Modo: **{modo}**")
        st.caption("🌍 Coleta sempre global")

        return selected


# ─────────────────────────────────────────
#  2. PAINEL PRINCIPAL
# ─────────────────────────────────────────

def render_main(selected_term: str | None) -> None:
    render_viral_alerts()

    hashtags = get_all_hashtags()

    if not hashtags:
        _render_empty_state()
        return

    st.divider()

    if st.session_state.get("show_compare"):
        render_compare_view(hashtags)
    elif selected_term:
        render_detail_view(selected_term)
    else:
        render_overview(hashtags)


# ─────────────────────────────────────────
#  3. VISAO GERAL
# ─────────────────────────────────────────

def render_overview(hashtags: list[dict]) -> None:
    col_img, col_txt = st.columns([0.5, 7])
    with col_img:
        try:
            st.image("assets/graph.png", width=100)
        except Exception:
            st.write("📊")
    with col_txt:
        st.title("Visao Geral")

    st.caption("Clique em uma hashtag na sidebar para detalhe. Use Comparar para comparar multiplas.")

    cols = st.columns(min(len(hashtags), 4))
    for i, entry in enumerate(hashtags):
        term   = entry["hashtag"]
        status = get_trend_status(term)
        color  = _get_color(i)

        with cols[i % 4]:
            avg_idx    = _get_avg_index(term)
            growth_24h = status.get("growth_24h")
            st.markdown(
                f"<h4 style='color:{color};margin-bottom:4px'>"
                f"{format_hashtag_display(term)}</h4>",
                unsafe_allow_html=True,
            )
            st.metric(
                label="Media do indice",
                value=f"{avg_idx}/100" if avg_idx is not None else "N/A",
                delta=format_percentage(growth_24h) if growth_24h is not None else None,
                help="Media de todas as coletas — mais estavel que o valor instantaneo.",
            )
            st.caption(status.get("status_label", ""))

    st.divider()
    render_multi_line_chart(hashtags)
    st.divider()
    render_ranking()


def render_multi_line_chart(hashtags: list[dict]) -> None:
    """
    Grafico comparativo com suavizacao por media movel.

    ESTRATEGIA DE ORDENACAO:
    Com multiplas traces e eixo categorico, o Plotly define a ordem das
    categorias baseado na PRIMEIRA trace adicionada. Se hashtags diferentes
    tem coletas em datas diferentes, o eixo fica fora de ordem.

    Solucao: construir um eixo X global com TODAS as datas unicas de todas
    as hashtags, ordenadas cronologicamente. Cada trace usa esse eixo
    compartilhado — datas sem coleta ficam como None (linha nao conectada).
    """
    st.subheader("📈 Comparativo de Tendências")

    show_smooth = st.toggle(
        "Suavizar com media movel",
        value=True,
        help=(
            "Ativado: media das ultimas 3 coletas — mais estavel. "
            "Desativado: valor exato de cada coleta."
        ),
    )

    # ── Passo 1: coleta todos os registros de todas as hashtags ──────────────
    all_data = {}  # term -> list of records (ordenados)
    for entry in hashtags:
        term    = entry["hashtag"]
        records = get_stats_history(term, limit=50)
        if records:
            all_data[term] = _sort_records_chronological(records)

    if not all_data:
        st.info("Faca uma coleta para ver o grafico comparativo.")
        return

    # ── Passo 2: constroi eixo X global com todas as datas unicas, ordenadas ─
    # Usa o timestamp ISO completo como chave de ordenacao
    all_timestamps_iso = set()
    for records in all_data.values():
        for r in records:
            all_timestamps_iso.add(r["collected_at"])

    # Ordena por datetime real — garante ordem cronologica independente de qual
    # hashtag tem mais ou menos coletas
    sorted_iso = sorted(all_timestamps_iso, key=lambda d: _to_datetime(d))

    # Converte para string formatada so para exibicao
    x_axis = [_fmt_date(ts) for ts in sorted_iso]

    # Mapa de ISO -> indice no eixo X (para lookup rapido)
    iso_to_idx = {ts: i for i, ts in enumerate(sorted_iso)}

    # ── Passo 3: plota cada hashtag alinhada ao eixo global ──────────────────
    fig = go.Figure()

    for i, (term, records) in enumerate(all_data.items()):
        color = _get_color(i)

        # Cria arrays do tamanho do eixo X global, preenchidos com None
        raw_vals = [None] * len(sorted_iso)
        for r in records:
            idx = iso_to_idx.get(r["collected_at"])
            if idx is not None:
                raw_vals[idx] = r["views_total"] or 0

        if show_smooth:
            # Calcula media movel apenas nos valores nao-None
            filled = _fill_nones_for_moving_avg(raw_vals)
            plot_values = _calculate_moving_average(filled, window=3)
            # Restaura None onde nao havia dados originais
            plot_values = [v if raw_vals[j] is not None else None
                           for j, v in enumerate(plot_values)]
            line_style = dict(color=color, width=3)
        else:
            plot_values = raw_vals
            line_style  = dict(color=color, width=2, dash="dot")

        fig.add_trace(go.Scatter(
            x=x_axis,
            y=plot_values,
            name=format_hashtag_display(term),
            mode="lines+markers",
            line=line_style,
            marker=dict(size=7),
            connectgaps=True,  # conecta os pontos existentes, pulando os None
            hovertemplate=(
                f"<b>{format_hashtag_display(term)}</b><br>"
                "Data: %{x}<br>Indice: %{y}<extra></extra>"
            ),
        ))

        # Linha fantasma dos valores brutos quando suavizado
        if show_smooth:
            fig.add_trace(go.Scatter(
                x=x_axis,
                y=raw_vals,
                name=f"{format_hashtag_display(term)} (bruto)",
                mode="lines",
                line=dict(color=color, width=1, dash="dot"),
                opacity=0.25,
                showlegend=False,
                hoverinfo="skip",
                connectgaps=True,
            ))

    label_y = "Indice Suavizado (media 3 coletas)" if show_smooth else "Indice Bruto (0-100)"

    fig.update_layout(
        xaxis_title="Data da Coleta",
        xaxis=dict(
            type="category",
            categoryorder="array",
            categoryarray=x_axis,  # FORÇA a ordem que definimos
            showgrid=True,
            gridwidth=1,
            gridcolor="#313244",
            tickangle=-35,
        ),
        yaxis_title=label_y,
        yaxis=dict(range=[0, 105], showgrid=True, gridwidth=1, gridcolor="#313244"),
        legend=dict(orientation="h", y=-0.25),
        hovermode="x unified",
        template="plotly_dark",
        height=550,
        margin=dict(l=10, r=10, t=10, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    if show_smooth:
        st.caption(
            "📌 Linha solida = media das ultimas 3 coletas. "
            "Linha pontilhada fina = valores brutos. "
            "Indice 100 = pico maximo do periodo."
        )
    else:
        st.caption("📌 Valores brutos de cada coleta. Use a media movel para ver a tendencia real.")


def _fill_nones_for_moving_avg(values: list) -> list[float]:
    """
    Substitui None por interpolacao linear entre vizinhos para calculo
    da media movel. Necessario porque a media movel nao funciona com None.
    O resultado com None e restaurado apos o calculo.
    """
    result = list(values)
    n = len(result)
    for i in range(n):
        if result[i] is None:
            # Busca vizinho anterior e proximo validos
            prev = next((result[j] for j in range(i-1, -1, -1) if result[j] is not None), None)
            nxt  = next((result[j] for j in range(i+1, n)      if result[j] is not None), None)
            if prev is not None and nxt is not None:
                result[i] = (prev + nxt) / 2
            elif prev is not None:
                result[i] = prev
            elif nxt is not None:
                result[i] = nxt
            else:
                result[i] = 0
    return result


# ─────────────────────────────────────────
#  4. MODO COMPARACAO
# ─────────────────────────────────────────

def render_compare_view(hashtags: list[dict]) -> None:
    col_title, col_close = st.columns([5, 1])
    with col_title:
        st.title("⚖️ Comparar Hashtags")
    with col_close:
        st.write("")
        if st.button("✕ Fechar", key="btn_close_compare"):
            st.session_state.show_compare = False
            st.rerun()

    st.caption("Selecione ate 5 hashtags para comparar no mesmo grafico.")

    all_terms = [h["hashtag"] for h in hashtags]

    if "compare_selected" not in st.session_state:
        st.session_state.compare_selected = []

    st.markdown("#### Selecione as hashtags:")

    new_selection = []
    for i, term in enumerate(all_terms):
        color     = _get_color(i)
        is_marked = term in st.session_state.compare_selected
        disabled  = len(st.session_state.compare_selected) >= 5 and not is_marked

        latest  = get_latest_stats(term)
        idx     = latest.get("views_total") if latest else None
        idx_str = f" — indice atual: **{idx}**" if idx is not None else ""

        col_check, col_info = st.columns([1, 4])
        with col_check:
            checked = st.checkbox(
                label=format_hashtag_display(term),
                value=is_marked,
                key=f"cmp_{term}",
                disabled=disabled,
            )
        with col_info:
            st.markdown(
                f"<span style='color:{color};font-size:1.05rem'>"
                f"{format_hashtag_display(term)}</span>"
                f"<span style='color:#888;font-size:0.9rem'>{idx_str}</span>",
                unsafe_allow_html=True,
            )

        if checked:
            new_selection.append(term)

    st.session_state.compare_selected = new_selection[:5]

    if len(new_selection) > 5:
        st.warning("Limite de 5 hashtags atingido.")

    st.divider()

    if st.button(
        "Gerar Grafico de Comparacao",
        type="primary",
        disabled=len(st.session_state.compare_selected) < 2,
        use_container_width=True,
    ):
        pass

    if len(st.session_state.compare_selected) < 2:
        st.caption("Selecione ao menos 2 hashtags para gerar o grafico.")
    else:
        _render_compare_chart(st.session_state.compare_selected)
        _render_compare_table(st.session_state.compare_selected)


def _render_compare_chart(terms: list[str]) -> None:
    """
    Grafico comparativo dos termos selecionados.
    Usa timestamps ISO para ordenacao correta no Plotly.
    """
    st.subheader("📈 Grafico Comparativo")

    all_hashtags = get_all_hashtags()
    all_terms    = [h["hashtag"] for h in all_hashtags]

    fig      = go.Figure()
    has_data = False

    for term in terms:
        records = get_stats_history(term, limit=50)
        if not records:
            st.warning(f"{format_hashtag_display(term)}: sem dados.")
            continue

        records    = _sort_records_chronological(records)
        values     = [r["views_total"] or 0 for r in records]
        # Ordena primeiro, formata depois — eixo categorico sem espacos vazios
        timestamps = [_fmt_date(r["collected_at"]) for r in records]
        color      = _get_color(all_terms.index(term) if term in all_terms else 0)
        has_data   = True

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=values,
            name=format_hashtag_display(term),
            mode="lines+markers",
            line=dict(color=color, width=2),
            marker=dict(size=7),
            hovertemplate=(
                f"<b>{format_hashtag_display(term)}</b><br>"
                "Data: %{x}<br>Indice: %{y}<extra></extra>"
            ),
        ))

    if not has_data:
        st.info("Nenhum dos termos tem dados. Faca uma coleta primeiro.")
        return

    fig.update_layout(
        xaxis_title="Data da Coleta",
        xaxis=dict(
            type="category",
            showgrid=True,
            gridwidth=1,
            gridcolor="#313244",
            tickangle=-35,
        ),
        yaxis_title="Indice de Interesse (0-100)",
        yaxis=dict(range=[0, 105]),
        legend=dict(orientation="h", y=-0.25),
        hovermode="x unified",
        template="plotly_dark",
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("📌 Indice 100 = pico maximo do periodo para cada termo individualmente.")


def _render_compare_table(terms: list[str]) -> None:
    st.subheader("📋 Resumo da Comparacao")
    rows = []
    for term in terms:
        status = get_trend_status(term)
        rows.append({
            "Hashtag": format_hashtag_display(term),
            "Indice":  status.get("current") if status.get("current") is not None else "—",
            "24h":     format_percentage(status.get("growth_24h")),
            "7d":      format_percentage(status.get("growth_7d")),
            "Status":  status.get("status_label", "—"),
            "Viral":   "🔥" if status.get("is_viral") else "",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ─────────────────────────────────────────
#  5. VISAO DETALHE
# ─────────────────────────────────────────

def render_detail_view(term: str) -> None:
    hashtags = get_all_hashtags()
    color    = _get_color(
        next((i for i, h in enumerate(hashtags) if h["hashtag"] == term), 0)
    )
    status = get_trend_status(term)
    latest = get_latest_stats(term)

    st.markdown(
        f"<h1 style='color:{color}'>{format_hashtag_display(term)}</h1>",
        unsafe_allow_html=True,
    )
    col_s, col_u = st.columns([2, 3])
    with col_s:
        st.markdown(f"**{status['status_label']}**")
    with col_u:
        if status["last_updated"]:
            st.caption(f"Ultima coleta: {format_datetime_display(status['last_updated'])}")

    st.divider()

    current    = status.get("current")
    growth_24h = status.get("growth_24h")
    growth_7d  = status.get("growth_7d")
    avg_views  = latest.get("avg_views") if latest else None
    delta_24h  = format_percentage(growth_24h) if growth_24h is not None else None
    delta_7d   = format_percentage(growth_7d)  if growth_7d  is not None else None

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("📊 Indice Atual",    f"{current}/100" if current is not None else "N/A")
    with c2:
        st.metric("⏱️ Crescimento 24h", delta_24h or "N/A", delta=delta_24h)
    with c3:
        st.metric("📅 Crescimento 7d",  delta_7d  or "N/A", delta=delta_7d)
    with c4:
        st.metric("📉 Media 7d",        f"{avg_views:.1f}" if avg_views else "N/A")

    st.divider()

    render_related_rising(term)

    st.divider()

    tab1, tab2, tab3 = st.tabs(["📈 Tendencia", "📊 Crescimento Diario", "🗃️ Historico"])
    with tab1:
        render_line_chart(term, color)
    with tab2:
        render_growth_chart(term)
    with tab3:
        render_history_table(term)

    st.divider()
    render_ranking()


# ─────────────────────────────────────────
#  6. TERMOS RELACIONADOS EM ALTA
# ─────────────────────────────────────────

def render_related_rising(term: str) -> None:
    with st.spinner("Buscando termos relacionados..."):
        related = _get_related_rising(term)

    if not related:
        st.info("Nenhum termo relacionado em alta encontrado para este periodo.")
        return

    st.subheader("🔥 Termos Relacionados em Alta")
    st.caption(
        "Termos que estao crescendo rapidamente junto com "
        f"{format_hashtag_display(term)}. "
        "'Breakout' = crescimento acima de 5000%."
    )

    cols = st.columns(2)
    for i, item in enumerate(related):
        related_term = item.get("term", "")
        growth_val   = item.get("growth", "")

        if growth_val == "Breakout" or growth_val == "100":
            growth_display = "🚀 Breakout"
            badge_color    = "#ff4444"
        else:
            try:
                pct = int(growth_val)
                growth_display = f"+{pct}%"
                badge_color = "#ff6b35" if pct >= 500 else "#f5a623" if pct >= 200 else "#4caf50"
            except ValueError:
                growth_display = growth_val
                badge_color    = "#888"

        with cols[i % 2]:
            st.markdown(f"""
                <div style="background:#1e1e2e;border:1px solid #313244;
                            border-radius:8px;padding:10px 14px;margin:4px 0;
                            display:flex;justify-content:space-between;align-items:center">
                    <span style="color:#ddd;font-size:0.95rem">🔍 {related_term}</span>
                    <span style="color:{badge_color};font-weight:bold;font-size:1rem">
                        {growth_display}
                    </span>
                </div>
            """, unsafe_allow_html=True)


# ─────────────────────────────────────────
#  7. GRAFICOS INDIVIDUAIS
# ─────────────────────────────────────────

def render_line_chart(term: str, color: str = "#7c85f5") -> None:
    """
    Linha de tendencia com media movel.
    Usa timestamps ISO para ordenacao correta no Plotly.
    """
    data = get_moving_average_series(term, limit=50)

    if not data["dates"]:
        st.info("Sem historico suficiente. Faca ao menos 2 coletas.")
        return

    # Ordena por datetime real antes de plotar
    # Ordena por datetime real, depois converte para string categorica
    paired = sorted(
        zip(data["dates"], data["values"], data["moving_average"]),
        key=lambda x: _to_datetime(x[0]),
    )
    timestamps = [_fmt_date(p[0]) for p in paired]  # string formatada para eixo categorico
    values     = [p[1] for p in paired]
    mov_avg    = [p[2] for p in paired]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=timestamps, y=values,
        name="Indice Real",
        mode="lines+markers",
        line=dict(color=color, width=2, dash="dot"),
        marker=dict(size=5),
        hovertemplate="Data: %{x}<br>Indice: %{y}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=timestamps, y=mov_avg,
        name="Media Movel",
        mode="lines",
        line=dict(color="#f5a623", width=3),
        hovertemplate="Data: %{x}<br>Media: %{y}<extra></extra>",
    ))
    fig.update_layout(
        title=f"Tendencia — {format_hashtag_display(term)}",
        xaxis_title="Data da Coleta",
        xaxis=dict(type="category", tickangle=-35),
        yaxis_title="Indice (0-100)",
        yaxis=dict(range=[0, 105]),
        legend=dict(orientation="h", y=-0.25),
        hovermode="x unified",
        template="plotly_dark",
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("📌 Indice 100 = pico maximo do periodo. Valores sao relativos.")


def render_growth_chart(term: str) -> None:
    """Barras de crescimento % — ordenacao por datetime real."""
    records = get_stats_history(term, limit=30)
    if len(records) < 2:
        st.info("Sem historico suficiente. Faca ao menos 2 coletas.")
        return

    records = _sort_records_chronological(records)
    timestamps, growths = [], []

    for i in range(1, len(records)):
        old_val = records[i-1].get("views_total") or 0
        new_val = records[i].get("views_total")   or 0
        growth  = round(((new_val - old_val) / old_val) * 100, 2) if old_val > 0 else 0
        # Formata apos ordenar — eixo categorico sem espacos vazios
        timestamps.append(_fmt_date(records[i]["collected_at"]))
        growths.append(growth)

    colors = ["#4caf50" if g >= 0 else "#f44336" for g in growths]

    fig = go.Figure(go.Bar(
        x=timestamps, y=growths,
        marker_color=colors,
        text=[f"{g:+.1f}%" for g in growths],
        textposition="outside",
    ))
    fig.update_layout(
        title=f"Crescimento entre Coletas — {format_hashtag_display(term)}",
        xaxis_title="Data da Coleta",
        xaxis=dict(type="category", tickangle=-35),
        yaxis_title="Variacao (%)",
        template="plotly_dark",
        height=400,
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_history_table(term: str) -> None:
    records = get_stats_history(term, limit=100)
    if not records:
        st.info("Nenhuma coleta registrada ainda.")
        return

    st.caption(f"{len(records)} registro(s) — clique em 🗑️ para deletar uma coleta especifica.")

    h1, h2, h3, h4 = st.columns([2.5, 2, 2, 2])
    h1.markdown("**Data / Hora**")
    h2.markdown("**Indice**")
    h3.markdown("**Media 7d**")
    h4.markdown("**Del**")
    st.markdown("---")

    for r in records:
        c1, c2, c3, c4 = st.columns([2.5, 1.9, 1.9, 1.9])
        c1.write(format_datetime_display(r["collected_at"]))
        c2.write(str(r["views_total"]) if r["views_total"] is not None else "—")
        c3.write(f"{r['avg_views']:.1f}" if r["avg_views"] else "—")
        if c4.button("🗑️", key=f"del_stat_{r['id']}"):
            _handle_delete_stat(r["id"], term)


def render_viral_alerts() -> None:
    virals = get_viral_alerts()
    if not virals:
        return
    st.subheader("🚨 Trending Topics Detectados")
    cols = st.columns(min(len(virals), 4))
    for i, viral in enumerate(virals):
        with cols[i % 4]:
            growth_str   = format_percentage(viral.get("growth_24h"))
            term_display = format_hashtag_display(viral["term"])
            current      = viral.get("current") or 0
            st.markdown(f"""
                <div style="background:#2d1b1b;border-left:4px solid #ff4444;
                            border-radius:4px;padding:10px 14px;margin:6px 0">
                    <strong>{term_display}</strong><br/>
                    <span style="color:#ff6b6b;font-size:1.2rem">↑ {growth_str}</span>
                    <span style="color:#888;font-size:0.8rem"> (24h)</span><br/>
                    <span style="color:#aaa;font-size:0.85rem">Indice: {current}/100</span>
                </div>
            """, unsafe_allow_html=True)


def render_ranking() -> None:
    """Ranking ordenado pela MEDIA do indice — mais justo que o valor instantaneo."""
    st.subheader("🏆 Ranking Geral")
    hashtags = get_all_hashtags()
    if not hashtags:
        st.info("Adicione termos e faca uma coleta para ver o ranking.")
        return

    ranking_data = []
    for entry in hashtags:
        term    = entry["hashtag"]
        avg_idx = _get_avg_index(term)
        status  = get_trend_status(term)
        ranking_data.append({
            "term":    term,
            "avg_idx": avg_idx if avg_idx is not None else 0,
            "status":  status,
        })

    ranking_data.sort(key=lambda x: (
        not x["status"].get("is_viral", False),
        -(x["avg_idx"]),
    ))

    rows = []
    for i, item in enumerate(ranking_data, 1):
        status  = item["status"]
        avg_idx = item["avg_idx"]
        rows.append({
            "#":            i,
            "Termo":        format_hashtag_display(item["term"]),
            "Media indice": f"{avg_idx}" if avg_idx else "—",
            "24h":          format_percentage(status.get("growth_24h")),
            "7d":           format_percentage(status.get("growth_7d")),
            "Status":       status.get("status_label", "—"),
            "Viral":        "🔥" if status.get("is_viral") else "",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption("📌 Ordenado pela media de todas as coletas — nao pelo valor instantaneo.")


# ─────────────────────────────────────────
#  8. HANDLERS
# ─────────────────────────────────────────

def _handle_add_hashtag(term: str) -> None:
    if not is_valid_hashtag(term):
        st.sidebar.error(f"'{term}' invalido. Use letras, numeros e underscore (min. 2 chars).")
        return
    result = add_hashtag(term)
    if result["success"]:
        st.sidebar.success(result["message"])
        st.rerun()
    else:
        st.sidebar.warning(result["message"])


def _handle_remove_hashtag(term: str) -> None:
    result = remove_hashtag(term)
    if result["success"]:
        st.sidebar.success(result["message"])
        if st.session_state.get("selected_term") == term:
            st.session_state.selected_term = None
        if term in st.session_state.get("compare_selected", []):
            st.session_state.compare_selected.remove(term)
        cache_key = f"related_{term}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        st.rerun()
    else:
        st.sidebar.error(result["message"])


def _handle_delete_stat(stat_id: int, term: str) -> None:
    try:
        conn = get_connection()
        conn.execute("DELETE FROM hashtag_stats WHERE id = ?", (stat_id,))
        conn.commit()
        conn.close()
        st.success("Coleta removida com sucesso.")
        st.rerun()
    except Exception as e:
        st.error(f"Erro ao deletar coleta: {e}")


def _handle_import_file(uploaded_file) -> None:
    try:
        content = uploaded_file.read().decode("utf-8")
        lines   = [l.strip() for l in content.splitlines() if l.strip()]
        if not lines:
            st.sidebar.warning("Arquivo vazio.")
            return
        result = import_hashtags_from_list(lines)
        st.sidebar.success(
            f"Importacao: {result['added']} adicionadas, "
            f"{result['skipped']} ja existiam, {result['errors']} erros."
        )
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Erro ao importar: {e}")


def _handle_run_collection() -> None:
    if not get_all_hashtags():
        st.sidebar.warning("Adicione ao menos um termo antes de coletar.")
        return
    keys_to_del = [k for k in st.session_state if k.startswith("related_")]
    for k in keys_to_del:
        del st.session_state[k]
    with st.sidebar:
        with st.spinner("Coletando dados (global)..."):
            result = run_collection(geo="")
        st.success(
            f"✓ {result['success']} coletados"
            + (f" · 🔥 {result['viral_count']} virais" if result["viral_count"] else "")
        )
    st.rerun()


# ─────────────────────────────────────────
#  9. TELA VAZIA
# ─────────────────────────────────────────

def _render_empty_state() -> None:
    st.markdown("""
        <div style="text-align:center;padding:60px 20px">
            <h1>🔥 Trend Monitor</h1>
            <p style="font-size:1.2rem;color:#aaa">
                Monitore tendencias em tempo real com Google Trends
            </p>
            <p style="color:#888">← Adicione um termo na barra lateral para comecar</p>
        </div>
    """, unsafe_allow_html=True)
    st.subheader("💡 Sugestoes para comecar")
    suggestions = [
        "gaming", "anime", "valorant", "genshinimpact",
        "wutheringwaves", "minecraft", "music", "dance",
    ]
    cols = st.columns(4)
    for i, s in enumerate(suggestions):
        with cols[i % 4]:
            if st.button(format_hashtag_display(s), key=f"sug_{s}", use_container_width=True):
                if add_hashtag(s)["success"]:
                    st.rerun()


# ─────────────────────────────────────────
#  10. PONTO DE ENTRADA
# ─────────────────────────────────────────

def run_dashboard() -> None:
    st.markdown("""
        <style>
            .block-container { padding-top: 1.5rem; }
            [data-testid="metric-container"] {
                background-color: #1e1e2e;
                border: 1px solid #313244;
                border-radius: 8px;
                padding: 12px;
            }
        </style>
    """, unsafe_allow_html=True)

    if "show_compare" not in st.session_state:
        st.session_state.show_compare = False
    if "compare_selected" not in st.session_state:
        st.session_state.compare_selected = []

    selected = render_sidebar()
    render_main(selected)