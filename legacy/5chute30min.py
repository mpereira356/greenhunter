# -*- coding: utf-8 -*-
"""
Bot V2 (corrigido):
- Chave anti-duplicação baseada no ID do jogo (/r/<id>) + persistência em JSON.
- Detecção de RED no fim do 1º tempo (HT/Intervalo/Half Time OU tempo ~45–47').
- GREEN só conta se o gol sair ainda no 1º tempo.
- Sessão HTTP com retry/backoff e timeout.
- Compatível com urllib3 antigo (method_whitelist).
- Salva resultados em Excel a cada GREEN/RED.
- Envia a planilha Excel apenas 1 vez por dia, por volta das 23h.
"""

import os
import re
import json
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================== CONFIG BOT ====================
token = "7275626989:AAExU9H0Fbu4FahbF1nNLcIVR6HqBTrgTjY"
chat_id = -4279039286

# ==================== PERSISTÊNCIA ====================
ALERTAS_JSON = "jogos_alertados_v2.json"         # jogos pendentes acompanhando (por ID)
HISTORICO_XLSX = "historico_jogos_v2.xlsx"       # histórico salvo
SENT_IDS_FILE = "jogos_enviados_ids.json"        # IDs já alertados (para nunca duplicar)

def agora_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def carregar_alertas():
    if os.path.exists(ALERTAS_JSON):
        try:
            with open(ALERTAS_JSON, "r", encoding="utf-8") as f:
                d = json.load(f)
                return dict(d)
        except Exception:
            return {}
    return {}

def salvar_alertas(d):
    try:
        with open(ALERTAS_JSON, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
    except Exception as e:
        print(f"[{agora_str()}] Erro ao salvar {ALERTAS_JSON}: {e}")

def carregar_ids_enviados():
    if os.path.exists(SENT_IDS_FILE):
        try:
            with open(SENT_IDS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def salvar_ids_enviados(s):
    try:
        with open(SENT_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(s), f, ensure_ascii=False)
    except Exception as e:
        print(f"[{agora_str()}] Erro ao salvar {SENT_IDS_FILE}: {e}")

# ==================== HTTP RESILIENTE ====================
def make_session():
    sess = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        # compatível com urllib3 antigo
        method_whitelist=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })
    return sess

session = make_session()

# ==================== TELEGRAM ====================
def enviar_telegram(token, chat_id, texto):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {'chat_id': chat_id, 'text': texto, 'parse_mode': 'Markdown'}
    try:
        session.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"[{agora_str()}] Erro ao enviar Telegram: {e}")

def enviar_arquivo_excel(token, chat_id, caminho_arquivo):
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    if not os.path.exists(caminho_arquivo):
        return
    try:
        with open(caminho_arquivo, "rb") as f:
            files = {'document': f}
            data = {'chat_id': chat_id}
            session.post(url, files=files, data=data, timeout=30)
    except Exception as e:
        print(f"[{agora_str()}] Erro ao enviar arquivo: {e}")

# ==================== UTILS ====================
def extrair_valor_td(td):
    span = td.find("span", class_="sr-only")
    return span.get_text(strip=True) if span else td.get_text(strip=True)

def salvar_jogo_excel(info, status, estatisticas):
    linha = {
        "Campeonato": info.get("campeonato", "?"),
        "Time Casa": info["time_casa"],
        "Time Visitante": info["time_visitante"],
        "Placar Inicial": info["placar_inicial"],
        "Placar Final": info.get("placar_final", info["placar_inicial"]),
        "Status": status,
        "Tempo": info.get("tempo", ""),
        "URL": info["url"]
    }
    for k, v in estatisticas.items():
        k_str = str(k)
        if isinstance(v, tuple) and len(v) == 2:
            linha[f"{k_str} (Casa)"] = v[0]
            linha[f"{k_str} (Fora)"] = v[1]
        else:
            linha[k_str] = v

    df_linha = pd.DataFrame([linha])
    if os.path.exists(HISTORICO_XLSX):
        try:
            df_existente = pd.read_excel(HISTORICO_XLSX)
            df_final = pd.concat([df_existente, df_linha], ignore_index=True, sort=False)
        except Exception as e:
            print(f"[{agora_str()}] Erro ao ler/concatenar Excel: {e}. Criando novo.")
            df_final = df_linha
    else:
        df_final = df_linha

    try:
        df_final.to_excel(HISTORICO_XLSX, index=False)
    except Exception as e:
        print(f"[{agora_str()}] Erro ao salvar Excel: {e}")

def eh_intervalo_primeiro_tempo(tempo_str: str, tempo_int: int) -> bool:
    """
    Considera fim do 1º tempo quando:
    - aparece 'HT', 'Half Time' ou 'Intervalo' no texto, OU
    - o tempo numérico está entre 45 e 47 minutos (compensação de acréscimo).
    NÃO considera tempos maiores (2º tempo).
    """
    s = (tempo_str or "").lower()
    if "ht" in s or "half" in s or "intervalo" in s:
        return True
    # evita tratar 2º tempo como intervalo
    if 45 <= tempo_int <= 47:
        return True
    return False

def gol_no_primeiro_tempo(tempo_str: str, tempo_int: int) -> bool:
    """
    Retorna True se o gol ocorreu no 1º tempo.
    - Considera 0–45min (incluindo acréscimos até ~47).
    - Se o texto indicar 2º tempo claramente, retorna False.
    """
    s = (tempo_str or "").lower()

    # Se o próprio texto já fala 2º tempo, não é 1T
    indicadores_2t = ["2º", "2o", "2h", "2nd", "2nd half", "2nd-half"]
    if any(ind in s for ind in indicadores_2t):
        return False

    # Considera 0 até 47 ainda como 1º tempo
    if 0 <= tempo_int <= 47:
        return True

    return False

# ==================== CONFIG SITE ====================
url_base = "https://pt.betsapi.com"

# ==================== PARÂMETROS ====================
SOT_MINIMO = 5
TEMPO_MAXIMO = 30
DELAY_ENTRE_LISTA = 15
DELAY_ENTRE_JOGOS = 1.5

# ==================== ESTADO ====================
jogos_alertados = carregar_alertas()      # { game_id: info }
ids_enviados = carregar_ids_enviados()   # { game_id, ... }
site_offline = False

# controle de envio diário da planilha
ultima_data_envio = ""

# Mensagem inicial
enviar_telegram(token, chat_id, "🤖 Bot V2 iniciado! Monitorando jogos ao vivo (5 SOT em < 30 min).")

# ==================== LOOP PRINCIPAL ====================
while True:
    try:
        # --- Envio diário da planilha, 1x por dia, por volta das 23h ---
        agora = datetime.now()
        hora_atual = agora.strftime("%H:%M")
        data_atual = agora.strftime("%Y-%m-%d")

        # Qualquer horário entre 23:00 e 23:59, apenas 1 vez por data
        if hora_atual.startswith("09") and ultima_data_envio != data_atual:
            enviar_arquivo_excel(token, chat_id, HISTORICO_XLSX)
            ultima_data_envio = data_atual

        # -------- Página principal --------
        try:
            response = session.get(url_base, timeout=10)
        except requests.exceptions.RequestException as e:
            if not site_offline:
                enviar_telegram(token, chat_id, f"❌ *Erro ao acessar a página principal!*\n`{e}`")
                site_offline = True
            time.sleep(120)
            continue

        if response.status_code != 200:
            if not site_offline:
                enviar_telegram(token, chat_id, f"❌ *Erro HTTP {response.status_code} na página principal!*")
                site_offline = True
            time.sleep(180)
            continue

        if site_offline:
            enviar_telegram(token, chat_id, "✅ *Site online novamente!* Monitoramento retomado.")
            site_offline = False

        soup = BeautifulSoup(response.text, "html.parser")
        trs = soup.find_all("tr", id=lambda x: x and x.startswith("r_"))

        # -------- Varre jogos da listagem --------
        for tr in trs:
            try:
                sport_td = tr.find("td", class_="sport_n")
                league_td = tr.find("td", class_="league_n")
                time_span = tr.find("span", class_="race-time")

                sport_a = sport_td.find("a") if sport_td else None
                league_a = league_td.find("a") if league_td else None
                league_name = league_a.text.strip() if league_a else ""
                race_time_text = time_span.get_text(strip=True) if time_span else ""

                # Só futebol, sem e-soccer e com tempo numérico
                if not (sport_a and sport_a.get("href") == "/c/soccer"):
                    continue
                if "esoccer" in league_name.lower():
                    continue
                if not race_time_text or not race_time_text[0].isdigit():
                    continue

                m = re.match(r"(\d+)", race_time_text)
                if not m:
                    continue
                tempo_numerico = int(m.group(1))
                if tempo_numerico > TEMPO_MAXIMO:
                    continue

                game_link_tag = tr.find("a", href=re.compile(r"^/r/\d+"))
                if not game_link_tag:
                    continue

                game_href = game_link_tag["href"]
                game_url = url_base + game_href
                m_id = re.search(r"/r/(\d+)", game_href)
                game_id = m_id.group(1) if m_id else None
                if not game_id:
                    continue

                # Se já alertou esse ID (nessa execução ou outras), pula
                if game_id in jogos_alertados or game_id in ids_enviados:
                    continue

                # Abrir página do jogo
                try:
                    game_response = session.get(game_url, timeout=10)
                except requests.exceptions.RequestException as e:
                    print(f"[{agora_str()}] Falha ao abrir jogo: {e}")
                    time.sleep(2)
                    continue
                if game_response.status_code != 200:
                    time.sleep(1)
                    continue

                soup_game = BeautifulSoup(game_response.text, "html.parser")
                campeonato_tag = soup_game.select_one("ol.breadcrumb li:nth-of-type(2) a")
                campeonato = campeonato_tag.text.strip() if campeonato_tag else "?"

                tabelas = soup_game.find_all("table", class_="table table-sm")
                if not tabelas:
                    time.sleep(0.8)
                    continue

                linhas = tabelas[0].find_all("tr")
                time_casa = linhas[0].find_all("td")[0].get_text(strip=True)
                time_visitante = linhas[0].find_all("td")[2].get_text(strip=True)

                baliza_casa = 0
                baliza_fora = 0
                gols = "0 x 0"
                escanteios = "0 x 0"
                estatisticas_completas = {}

                for linha in linhas:
                    cols = linha.find_all("td")
                    if len(cols) != 3:
                        continue
                    nome_est = cols[1].get_text(strip=True)
                    casa_val = extrair_valor_td(cols[0])
                    fora_val = extrair_valor_td(cols[2])
                    estatisticas_completas[nome_est] = (casa_val, fora_val)
                    if nome_est.lower() == "golos":
                        gols = f"{casa_val} x {fora_val}"
                    elif nome_est.lower() == "corners":
                        escanteios = f"{casa_val} x {fora_val}"
                    elif "à baliza" in nome_est.lower() or "a baliza" in nome_est.lower():
                        try:
                            baliza_casa = int(casa_val)
                            baliza_fora = int(fora_val)
                        except:
                            pass

                if (baliza_casa + baliza_fora) >= SOT_MINIMO:
                    enviar_telegram(token, chat_id, f"""📢 *Alerta V2: {SOT_MINIMO}+ Chutes à Baliza!*
⏱️ Tempo: {race_time_text} (Máx: {TEMPO_MAXIMO} min)
🏟️ {time_casa} vs {time_visitante}
🥅 Placar: {gols}
🎯 À Baliza: {baliza_casa} x {baliza_fora}
🚩 Escanteios: {escanteios}
🔗 [Abrir no site]({game_url})""")

                    jogos_alertados[game_id] = {
                        "url": game_url,
                        "time_casa": time_casa,
                        "time_visitante": time_visitante,
                        "placar_inicial": gols,
                        "status": "pendente",
                        "campeonato": campeonato,
                        "tempo_alerta": race_time_text,
                        "sot_alerta": baliza_casa + baliza_fora
                    }
                    salvar_alertas(jogos_alertados)

                    ids_enviados.add(game_id)
                    salvar_ids_enviados(ids_enviados)

                time.sleep(DELAY_ENTRE_JOGOS)

            except Exception as inner_e:
                print(f"[{agora_str()}] Erro jogo-lista: {inner_e}")
                time.sleep(1.0)
                continue

        # -------- Acompanhamento dos alertados --------
        for game_id, info in list(jogos_alertados.items()):
            try:
                if info.get("status") != "pendente":
                    continue

                r = session.get(info["url"], timeout=10)
                if r.status_code != 200:
                    time.sleep(1)
                    continue

                soup_check = BeautifulSoup(r.text, "html.parser")
                tempo_tag = soup_check.find("span", class_="race-time")
                tempo_str = tempo_tag.get_text(strip=True) if tempo_tag else "0"
                tempo_match = re.match(r"(\d+)", tempo_str)
                tempo_int = int(tempo_match.group(1)) if tempo_match else 0
                jogos_alertados[game_id]["tempo"] = tempo_str

                tabelas_check = soup_check.find_all("table", class_="table table-sm")
                novo_placar = info["placar_inicial"]
                estatisticas_completas = {}

                if tabelas_check:
                    linhas_check = tabelas_check[0].find_all("tr")
                    for linha in linhas_check:
                        cols = linha.find_all("td")
                        if len(cols) == 3:
                            nome = cols[1].get_text(strip=True)
                            casa = extrair_valor_td(cols[0])
                            fora = extrair_valor_td(cols[2])
                            estatisticas_completas[nome] = (casa, fora)
                            if nome.lower() == "golos":
                                novo_placar = f"{casa} x {fora}"

                # GREEN: mudou o placar após o alerta, mas SOMENTE no 1º tempo
                if novo_placar != info["placar_inicial"] and gol_no_primeiro_tempo(tempo_str, tempo_int):
                    enviar_telegram(token, chat_id, f"""✅ *GREEN – Gol no 1º Tempo!*
{info["time_casa"]} vs {info["time_visitante"]}
Placar no Alerta ({info["tempo_alerta"]} min): {info["placar_inicial"]}
Placar no Momento do Gol ({tempo_str}): {novo_placar}""")

                    jogos_alertados[game_id]["status"] = "green"
                    jogos_alertados[game_id]["placar_final"] = novo_placar
                    salvar_jogo_excel(jogos_alertados[game_id], "green", estatisticas_completas)

                    salvar_alertas(jogos_alertados)
                    del jogos_alertados[game_id]
                    salvar_alertas(jogos_alertados)
                    time.sleep(0.5)
                    continue

                # RED: fim do 1º tempo sem gol
                if eh_intervalo_primeiro_tempo(tempo_str, tempo_int):
                    enviar_telegram(token, chat_id, f"""❌ *RED – Fim do 1º Tempo sem Gol*
{info["time_casa"]} vs {info["time_visitante"]}
Placar: {info["placar_inicial"]}""")

                    jogos_alertados[game_id]["status"] = "red"
                    jogos_alertados[game_id]["placar_final"] = info["placar_inicial"]
                    salvar_jogo_excel(jogos_alertados[game_id], "red", estatisticas_completas)

                    salvar_alertas(jogos_alertados)
                    del jogos_alertados[game_id]
                    salvar_alertas(jogos_alertados)
                    time.sleep(0.5)
                    continue

                time.sleep(0.6)

            except Exception as follow_e:
                print(f"[{agora_str()}] Erro acompanhamento: {follow_e}")
                time.sleep(1.0)
                continue

    except Exception as e:
        print(f"[{agora_str()}] ⚠️ Erro no loop principal: {e}")

    time.sleep(DELAY_ENTRE_LISTA)
