# Trend Monitor

A real-time trend analysis tool powered by Google Trends.  
Built to help understand what people are searching for and how trends behave over time.


## About

I built this project to support content creation decisions.

Google Trends is useful, but unstable: the same query can return different values minutes apart.  
So instead of relying on snapshots, this tool continuously collects data, stores history, and extracts signals from it.

It tracks multiple terms, smooths the noise, and highlights when something is actually gaining traction.


## What it does

- Monitor multiple keywords at the same time  
- Store every collection in a local SQLite database  
- Apply moving average smoothing to reduce noise  
- Rank terms by historical performance (not just latest value)  
- Detect viral growth (+50% in 24h or +200% in 7 days)  
- Show related rising terms  
- Compare multiple terms on the same chart  
- Fully local (no API keys, no cloud)  


## How it works

The main challenge is that Google Trends data is relative (0–100 scale).  
The peak changes every time you query it.

To deal with that, the system uses:

- 30-day collection window instead of short windows  
- Moving average smoothing (3-point)  
- Historical mean ranking instead of snapshot ranking  
- MID (Google topic ID) instead of raw text when possible  


## Project structure

Each module has a single responsibility:

- scraper → only collects data  
- database → only reads/writes SQLite  
- analytics → calculates growth, averages, rankings  
- ui → renders everything in Streamlit  
- scheduler → runs the collection pipeline  

No module mixes responsibilities.


## Scraper (core logic)

The data collection follows a 4-step pipeline:

1. Try to resolve the term to a Google MID (topic)  
2. Fetch interest data (30-day window)  
3. Fallback to plain term if needed  
4. Return simulated data if everything fails (so the system never breaks)  

Using MID instead of text improves consistency a lot, because it avoids ambiguity.


## Database

SQLite, single access layer.

Tables:

- hashtags → monitored terms  
- hashtag_stats → every collected data point  

Important details:

- WAL mode enabled  
- indexed for time-based queries  
- row_factory for dict-style access  


## Analytics

This is where raw data becomes useful:

- Growth % → compares oldest vs newest in a window  
- Moving average → smooths noise  
- Trend direction → compares recent averages instead of single points  
- Viral detection → based on growth thresholds  
- Ranking → based on historical mean, not latest spike  


## UI

Built with Streamlit.

Three main views:

- Overview → all terms side-by-side  
- Detail → deep dive into one term  
- Compare → up to 5 terms on one chart  

There’s also a custom solution for Plotly’s X-axis ordering problem when multiple series have different timestamps.


## Scheduler

Runs the pipeline:

terms → scraper → database → analytics  

Can be manual or automatic (APScheduler).


## Setup

Requirements:

- Python 3.11+  

Install:

```bash
git clone https://github.com/your-username/trend-monitor.git
cd trend-monitor
pip install -r requirements.txt
python -m streamlit run main.py