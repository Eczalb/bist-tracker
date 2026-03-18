import os, json, time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────
GECMIS_GUN = 180
EMA_PERIYOTLARI = [9, 21, 50, 200]
RSI_PERIYOT = 14
MACD_HIZLI, MACD_YAVAS, MACD_SINYAL = 12, 26, 9
BOLLINGER_PERIYOT, BOLLINGER_STD = 20, 2
TIMEOUT_SANIYE = 15

# Sadece XU100 endeksi
ENDEKSLER = {"XU100": "XU100.IS"}

# BIST 100 icindeki KATILIM hisseleri
HISSELER = [
    "ASELS", "THYAO", "FROTO", "TOASO", "KCHOL", "ENKAI", "TUPRS",
    "EREGL", "SISE", "CCOLA", "TCELL", "AKSEN", "BIMAS", "TAVHL",
    "MGROS", "PGSUS", "GUBRF", "SAHOL", "TTRAK", "OTKAR", "ULKER",
    "LOGO", "AEFES", "ARCLK", "SOKM", "KORDS", "EKGYO", "ENJSA",
    "TTKOM", "SASA", "DOHOL", "NUHCM", "BUCIM", "AYGAZ", "CWENE",
    "CIMSA", "KLMSN", "TURSG", "EUPWR", "TKURU",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SINYAL_SPREADSHEET_ID = os.environ.get("SINYAL_SPREADSHEET_ID", "").strip()
# ─────────────────────────────────────────


def rsi_hesapla(seri, periyot=14):
    delta = seri.diff()
    kazan = delta.clip(lower=0).ewm(com=periyot-1, adjust=False).mean()
    kayip = (-delta.clip(upper=0)).ewm(com=periyot-1, adjust=False).mean()
    rs = kazan / kayip
    return (100 - (100 / (1 + rs))).round(2)


def macd_hesapla(seri, h=12, y=26, s=9):
    macd = seri.ewm(span=h, adjust=False).mean() - seri.ewm(span=y, adjust=False).mean()
    sinyal = macd.ewm(span=s, adjust=False).mean()
    return macd.round(2), sinyal.round(2), (macd - sinyal).round(2)


def bollinger_hesapla(seri, p=20, k=2):
    orta = seri.rolling(p).mean()
    std = seri.rolling(p).std()
    ust = (orta + k * std).round(2)
    alt = (orta - k * std).round(2)
    return ust, orta.round(2), alt, ((seri - alt) / (ust - alt)).round(4)


def sinyal_uret(son):
    puan = 0
    rsi = float(son.get("RSI", 50) or 50)
    hist = float(son.get("MACD_Hist", 0) or 0)
    bb = float(son.get("BB_Yuzde", 0.5) or 0.5)
    e9 = float(son.get("EMA9", 0) or 0)
    e21 = float(son.get("EMA21", 0) or 0)
    if rsi < 30: puan += 2
    elif rsi < 40: puan += 1
    elif rsi > 70: puan -= 2
    elif rsi > 60: puan -= 1
    if hist > 0: puan += 1
    elif hist < 0: puan -= 1
    if bb < 0.1: puan += 2
    elif bb > 0.9: puan -= 2
    if e9 > 0 and e21 > 0:
        puan += 1 if e9 > e21 else -1
    if puan >= 3: return "GUCLU AL"
    elif puan >= 1: return "AL"
    elif puan <= -3: return "GUCLU SAT"
    elif puan <= -1: return "SAT"
    return "BEKLE"


def multiindex_duzelt(ham, sembol):
    if not isinstance(ham.columns, pd.MultiIndex):
        return ham
    l0 = ham.columns.get_level_values(0).tolist()
    l1 = ham.columns.get_level_values(1).tolist()
    if sembol in l0: return ham.xs(sembol, axis=1, level=0)
    elif sembol in l1: return ham.xs(sembol, axis=1, level=1)
    elif "Close" in l0:
        ham.columns = ham.columns.droplevel(1); return ham
    else:
        ham.columns = ham.columns.droplevel(0); return ham


def endeks_cek(ad, sembol, gun=180):
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 50)
    try:
        ham = yf.download(sembol, start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"), interval="1d",
                          progress=False, auto_adjust=True, timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA endeks {ad}: {e}"); return pd.DataFrame()
    if ham is None or ham.empty: return pd.DataFrame()
    ham = multiindex_duzelt(ham, sembol)
    if "Close" not in ham.columns: return pd.DataFrame()
    df = ham.tail(gun).copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append({"Endeks": str(ad),
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row.get("Open", 0)), 2),
                "Yuksek": round(float(row.get("High", 0)), 2),
                "Dusuk": round(float(row.get("Low", 0)), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
                "Degisim": 0.0})
        except: continue
    if not rows: return pd.DataFrame()
    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round((result.at[i, "Kapanis"] - prev) / prev * 100, 2)
    print(f"  OK {ad}: {result.iloc[-1]['Kapanis']:,.2f} puan")
    return result


def veri_cek(hisse, gun=180):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 100)
    try:
        ham = yf.download(sembol, start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"), interval="1d",
                          progress=False, auto_adjust=True, timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA {hisse}: {e}"); return pd.DataFrame()
    if ham is None or ham.empty: return pd.DataFrame()
    ham = multiindex_duzelt(ham, sembol)
    if any(s not in ham.columns for s in ["Open","High","Low","Close","Volume"]):
        return pd.DataFrame()
    df = ham.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append({"Hisse": str(hisse),
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row["Open"]), 2),
                "Yuksek": round(float(row["High"]), 2),
                "Dusuk": round(float(row["Low"]), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
                "Degisim": 0.0})
        except: continue
    if not rows: return pd.DataFrame()
    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round((result.at[i, "Kapanis"] - prev) / prev * 100, 2)
    for p in EMA_PERIYOTLARI:
        result[f"EMA{p}"] = result["Kapanis"].ewm(span=p, adjust=False).mean().round(2)
    result["RSI"] = rsi_hesapla(result["Kapanis"], RSI_PERIYOT)
    result["MACD"], result["MACD_Sinyal"], result["MACD_Hist"] = macd_hesapla(
        result["Kapanis"], MACD_HIZLI, MACD_YAVAS, MACD_SINYAL)
    result["BB_Ust"], result["BB_Orta"], result["BB_Alt"], result["BB_Yuzde"] =         bollinger_hesapla(result["Kapanis"], BOLLINGER_PERIYOT, BOLLINGER_STD)
    result = result.tail(gun).reset_index(drop=True)
    print(f"  OK {hisse}: RSI={result.iloc[-1]['RSI']:.1f} Kapanis={result.iloc[-1]['Kapanis']} TL")
    return result


def yaz(svc, sid, sayfa, veri):
    svc.values().update(spreadsheetId=sid, range=f"'{sayfa}'!A1",
        valueInputOption="RAW", body={"values": veri}).execute()
    time.sleep(0.5)


def sayfa_hazirla(svc, sid, isimler):
    meta = svc.get(spreadsheetId=sid).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if isimler[0] not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=sid, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": isimler[0]},
                "fields": "title"}}]}).execute()
        mevcut[0] = isimler[0]
        time.sleep(1)
    eksik = [s for s in isimler[1:] if s not in mevcut]
    if eksik:
        svc.batchUpdate(spreadsheetId=sid, body={
            "requests": [{"addSheet": {"properties": {"title": s}}} for s in eksik]
        }).execute()
        time.sleep(1)


def guncelle(df_hisse, df_endeks_dict):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json: raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    svc = build("sheets", "v4", credentials=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # ── ANA TABLO (buyuk, tum ham veri) ──────────────────────────
    sayfa_hazirla(svc, SPREADSHEET_ID,
                  ["Tum Veri", "Ozet"] + [f"Endeks_{ad}" for ad in df_endeks_dict])

    cols = list(df_hisse.columns)
    rows_all = [[str(v) for v in r] for r in df_hisse.values.tolist()]
    yaz(svc, SPREADSHEET_ID, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows_all)
    print(f"  Ana tablo Tum Veri: {len(rows_all)} satir")

    for ad, df_e in df_endeks_dict.items():
        if df_e.empty: continue
        cols_e = list(df_e.columns)
        rows_e = [[str(v) for v in r] for r in df_e.values.tolist()]
        yaz(svc, SPREADSHEET_ID, f"Endeks_{ad}",
            [[f"Guncelleme: {now}"] + [""]*(len(cols_e)-1), cols_e] + rows_e)
        son = df_e.iloc[-1]
        print(f"  Ana tablo Endeks_{ad}: {son['Kapanis']:,.2f}")

    # Ozet — her hisse son gun
    ozet_cols = ["Hisse","Tarih","Kapanis","Degisim%","RSI","MACD_Hist",
                 "BB_Yuzde","EMA9","EMA21","EMA50","EMA200","SINYAL","Guncelleme"]
    ozet = [ozet_cols]
    for h in sorted(df_hisse["Hisse"].unique()):
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty: continue
        son = df_h.iloc[-1]
        sinyal = sinyal_uret(son)
        ozet.append([h, str(son["Tarih"]), str(son["Kapanis"]), str(son["Degisim"]),
                     str(son.get("RSI","")), str(son.get("MACD_Hist","")),
                     str(son.get("BB_Yuzde","")), str(son.get("EMA9","")),
                     str(son.get("EMA21","")), str(son.get("EMA50","")),
                     str(son.get("EMA200","")), sinyal, now])
    yaz(svc, SPREADSHEET_ID, "Ozet", ozet)
    al = sum(1 for r in ozet[1:] if "AL" in r[-2])
    sat = sum(1 for r in ozet[1:] if "SAT" in r[-2])
    print(f"  Ana tablo Ozet: {al} AL | {sat} SAT | {len(ozet)-1-al-sat} BEKLE")

    # ── NOTEBOOKLM TABLOSU (sadece son gun ozeti) ─────────────────
    if not SINYAL_SPREADSHEET_ID:
        return
    sayfa_hazirla(svc, SINYAL_SPREADSHEET_ID, ["Sinyaller"])

    # Endeks bilgisini ilk satira ekle
    nm_rows = [["=== BIST 100 KATILIM HISSELERI SINYAL TABLOSU ===",
                f"Guncelleme: {now}", "", "", "", "", "", "", "", "", "", "", ""]]

    # XU100 satiri
    xu = df_endeks_dict.get("XU100", pd.DataFrame())
    if not xu.empty:
        son_xu = xu.iloc[-1]
        nm_rows.append([f"XU100 ENDEKSI", str(son_xu["Tarih"]),
                        str(son_xu["Kapanis"]), str(son_xu["Degisim"]),
                        "", "", "", "", "", "", "", "ENDEKS", now])

    nm_rows.append(["---"] * 13)  # Bosluk satiri
    nm_rows.append(ozet_cols)     # Baslik
    nm_rows += ozet[1:]           # Hisse verileri (baslik haric)

    yaz(svc, SINYAL_SPREADSHEET_ID, "Sinyaller", nm_rows)
    print(f"  NotebookLM tablosu: {len(nm_rows)} satir (XU100 + {len(ozet)-1} hisse)")


def main():
    print("="*50)
    print(f"BIST 100 Katilim - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Hisse: {len(HISSELER)} | Periyot: {GECMIS_GUN} gun")
    print("="*50)

    print("\nEndeksler...")
    df_endeks_dict = {}
    for ad, sembol in ENDEKSLER.items():
        print(f"  {ad}...")
        df_endeks_dict[ad] = endeks_cek(ad, sembol, GECMIS_GUN)
        time.sleep(1)

    print("\nHisseler...")
    tum, hata = [], []
    for i, h in enumerate(HISSELER, 1):
        print(f"[{i}/{len(HISSELER)}] {h}...")
        df = veri_cek(h, GECMIS_GUN)
        if not df.empty: tum.append(df)
        else: hata.append(h)
        time.sleep(0.3)

    print(f"\nBasarili: {len(tum)} | Hata: {len(hata)}")
    if hata: print(f"Atlananlar: {hata}")
    if not tum: raise SystemExit("Hic veri yok!")

    df_hisse = pd.concat(tum, ignore_index=True).sort_values(["Hisse","Tarih"])
    print(f"Toplam: {len(df_hisse)} satir")
    guncelle(df_hisse, df_endeks_dict)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
