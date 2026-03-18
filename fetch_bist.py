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

ENDEKSLER = {"XU100": "XU100.IS"}

# BIST 100 icindeki KATILIM hisseleri
HISSELER = [
    "ASELS", "THYAO", "FROTO", "TOASO", "KCHOL", "ENKAI", "TUPRS",
    "EREGL", "SISE", "CCOLA", "TCELL", "AKSEN", "BIMAS", "TAVHL",
    "MGROS", "PGSUS", "GUBRF", "SAHOL", "TTRAK", "OTKAR", "ULKER",
    "LOGO", "AEFES", "ARCLK", "SOKM", "KORDS", "EKGYO", "ENJSA",
    "TTKOM", "SASA", "DOHOL", "NUHCM", "BUCIM", "AYGAZ", "CWENE",
    "CIMSA", "KLMSN", "EUPWR", "TKURU", "TURSG",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
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
        ham.columns = ham.columns.droplevel(1)
        return ham
    else:
        ham.columns = ham.columns.droplevel(0)
        return ham


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
            rows.append({
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row.get("Open", 0)), 2),
                "Yuksek": round(float(row.get("High", 0)), 2),
                "Dusuk": round(float(row.get("Low", 0)), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Degisim": 0.0,
            })
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
            rows.append({
                "Hisse": str(hisse),
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row["Open"]), 2),
                "Yuksek": round(float(row["High"]), 2),
                "Dusuk": round(float(row["Low"]), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
                "Degisim": 0.0,
            })
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


def yaz(svc, sayfa, veri):
    svc.values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"'{sayfa}'!A1",
        valueInputOption="RAW", body={"values": veri}).execute()
    time.sleep(0.5)


def sayfa_hazirla(svc, isimler):
    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]
    # Ilk sayfayi yeniden adlandir
    if isimler[0] not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": isimler[0]},
                "fields": "title"}}]}).execute()
        mevcut[0] = isimler[0]
        time.sleep(1)
    eksik = [s for s in isimler[1:] if s not in mevcut]
    if eksik:
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
            "requests": [{"addSheet": {"properties": {"title": s}}} for s in eksik]
        }).execute()
        time.sleep(1)


def guncelle(df_hisse, df_endeks):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json: raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    svc = build("sheets", "v4", credentials=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Sayfalari hazirla
    sayfa_hazirla(svc, ["Tum Veri", "Ozet", "Endeks_XU100"])

    # 1. TUM VERI — tum hisse gunluk verileri
    cols = list(df_hisse.columns)
    rows = [[str(v) for v in r] for r in df_hisse.values.tolist()]
    yaz(svc, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""] * (len(cols) - 1), cols] + rows)
    print(f"  Tum Veri: {len(rows)} satir")

    # 2. ENDEKS_XU100 — gunluk endeks verileri
    if not df_endeks.empty:
        e_cols = list(df_endeks.columns)
        e_rows = [[str(v) for v in r] for r in df_endeks.values.tolist()]
        yaz(svc, "Endeks_XU100",
            [[f"XU100 | Guncelleme: {now}"] + [""] * (len(e_cols) - 1), e_cols] + e_rows)
        son_xu = df_endeks.iloc[-1]
        print(f"  Endeks_XU100: {son_xu['Kapanis']:,.2f} puan ({son_xu['Degisim']:+.2f}%)")

    # 3. OZET — her hisse icin sadece SON GUN
    # Boyut: ~40 satir — NotebookLM icin ideal
    ozet_cols = ["Hisse", "Tarih", "Kapanis", "Degisim%",
                 "EMA9", "EMA21", "EMA50", "EMA200",
                 "RSI", "MACD_Hist", "BB_Yuzde",
                 "SINYAL", "Guncelleme"]
    ozet = [ozet_cols]
    for h in sorted(df_hisse["Hisse"].unique()):
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty: continue
        son = df_h.iloc[-1]
        sinyal = sinyal_uret(son)
        ozet.append([
            h,
            str(son["Tarih"]),
            str(son["Kapanis"]),
            str(son["Degisim"]),
            str(son.get("EMA9", "")), str(son.get("EMA21", "")),
            str(son.get("EMA50", "")), str(son.get("EMA200", "")),
            str(son.get("RSI", "")),
            str(son.get("MACD_Hist", "")),
            str(son.get("BB_Yuzde", "")),
            sinyal, now
        ])
    yaz(svc, "Ozet", ozet)
    al = sum(1 for r in ozet[1:] if "AL" in r[-2])
    sat = sum(1 for r in ozet[1:] if "SAT" in r[-2])
    print(f"  Ozet: {len(ozet)-1} hisse | {al} AL | {sat} SAT | {len(ozet)-1-al-sat} BEKLE")


def main():
    print("=" * 50)
    print(f"BIST 100 Katilim - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Hisse: {len(HISSELER)} | Periyot: {GECMIS_GUN} gun")
    print("=" * 50)

    print("\nXU100 cekiliyor...")
    df_endeks = endeks_cek("XU100", "XU100.IS", GECMIS_GUN)
    time.sleep(1)

    print("\nHisseler cekiliyor...")
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

    df_hisse = pd.concat(tum, ignore_index=True).sort_values(["Hisse", "Tarih"])
    print(f"Toplam: {len(df_hisse)} satir")
    guncelle(df_hisse, df_endeks)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
