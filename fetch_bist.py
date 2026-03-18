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
MACD_HIZLI = 12
MACD_YAVAS = 26
MACD_SINYAL = 9
BOLLINGER_PERIYOT = 20
BOLLINGER_STD = 2
TIMEOUT_SANIYE = 15

ENDEKSLER = {
    "XU100": "XU100.IS",
    "XKTUM": "XKTUM.IS",
}

HISSELER = [
    "AEFES", "AGESA", "AKCNS", "AKFGY", "AKGRT", "AKSA", "AKSEN",
    "ALARK", "ALBRK", "ALFAS", "ALGYO", "ALKIM", "ANSGR", "ARCLK",
    "ARDYZ", "ARSAN", "ASELS", "ASGYO", "ASTOR", "ATAKP", "ATATP", "ATGYO",
    "AVGYO", "AYCES", "AYEN", "AYGAZ", "AZTEK", "BAGFS", "BAKAB", "BANVT",
    "BERA", "BFREN", "BIENY", "BIGCH", "BIMAS", "BIOEN", "BIZIM", "BMSTL",
    "BNTAS", "BOSSA", "BOYP", "BRKO", "BRMEN", "BRKVY", "BRSAN", "BRYAT",
    "BSOKE", "BTCIM", "BUCIM", "BURCE", "BURVA", "BVSAN", "CCOLA", "CELHA",
    "CEMAS", "CEMTS", "CIMSA", "CLEBI", "CMENT", "CRFSA", "CUSAN",
    "CWENE", "DARDL", "DENGE", "DERHL", "DESA", "DEVA", "DGATE", "DGKLB",
    "DGNMO", "DITAS", "DMSAS", "DNISI", "DOAS", "DOBUR", "DOCO", "DOGUB",
    "DOHOL", "ECILC", "ECZYT", "EDATA", "EGEEN", "EGPRO", "EGSER",
    "EKGYO", "EMKEL", "EMNIS", "ENERY", "ENJSA", "ENKAI", "EPLAS", "ERBOS",
    "ERCB", "EREGL", "ERSU", "ESCAR", "ESCOM", "ESEN", "ETILR", "EUHOL",
    "EUPWR", "EYGYO", "FENER", "FLAP", "FMIZP", "FONET", "FORMT", "FRIGO",
    "FROTO", "FZLGY", "GARAN", "GARFA", "GEDZA", "GEREL", "GLBMD", "GLCVY",
    "GLYHO", "GMTAS", "GOODY", "GOZDE", "GRSEL", "GSDHO", "GSRAY", "GUBRF",
    "GWIND", "HALKB", "HATEK", "HDFGS", "HEDEF", "HLGYO",
    "HTTBT", "HUBVC", "HUNER", "HURGZ", "ICBCT", "ICUGS",
    "IHEVA", "IHGZT", "IHLAS", "IHLGM", "IHYAY", "IMASM", "INFO", "INTEM",
    "INVEO", "ISGYO", "ISSEN", "ISYAT", "IZFAS", "IZOCM",
    "JANTS", "KAPLM", "KAREL", "KARSN", "KARTN", "KATMR", "KAYSE", "KCAER",
    "KCHOL", "KENT", "KERVN", "KFEIN", "KGYO", "KIMMR", "KLGYO",
    "KLKIM", "KLMSN", "KLRHO", "KLSER", "KMPUR", "KNFRT", "KONYA", "KOPOL",
    "KORDS", "KRDMA", "KRDMB", "KRDMD", "KRGYO", "KRONT",
    "KRPLS", "KRSTL", "KRTEK", "KSTUR", "KTLEV", "KUTPO", "LIDER", "LIDFA",
    "LKMNH", "LOGO", "LRSHO", "LUKSK", "MAALT", "MACKO", "MAGEN", "MAKIM",
    "MAKTK", "MANAS", "MARKA", "MEDTR", "MEGAP", "MEKAG", "MEPET", "MERCN",
    "MERIT", "MERKO", "METRO", "MGROS", "MIATK", "MMCAS",
    "MNDRS", "MOBTL", "MPARK", "MRGYO", "MRSHL", "MSGYO", "MTRKS",
    "MZHLD", "NATEN", "NETAS", "NIBAS", "NTHOL", "NUGYO", "NUHCM",
    "OBASE", "ODAS", "ONCSM", "ONRYT", "ORCAY", "ORGE", "ORMA", "OSTIM",
    "OTKAR", "OYAKC", "OYLUM", "OYYAT", "OZGYO", "OZKGY", "PAMEL", "PAPIL",
    "PARSN", "PASEU", "PATEK", "PCILT", "PEKGY", "PGSUS", "PKART",
    "PNLSN", "POLHO", "POLTK", "PRZMA", "PSDTC", "PSGYO",
    "RAYSG", "RNPOL", "RODRG", "ROYAL", "RTALB", "RUBNS",
    "RYGYO", "RYSAS", "SAFKR", "SAHOL", "SAMAT", "SANEL", "SANFM", "SANKO",
    "SARKY", "SASA", "SAYAS", "SDTTR", "SELEC", "SEYKM", "SILVR",
    "SISE", "SKBNK", "SMRTG", "SNGYO", "SNKRN", "SODSN", "SOKM", "SONME",
    "SRVGY", "SUMAS", "SUWEN", "TABGD", "TARKM", "TATGD", "TAVHL", "TBORG",
    "TCELL", "TDGYO", "TEKTU", "THYAO", "TKNSA", "TKURU", "TLMAN",
    "TMPOL", "TOASO", "TRCAS", "TRGYO", "TRILC", "TSKB", "TSPOR", "TTKOM",
    "TTRAK", "TUCLK", "TUPRS", "TUREX", "TURGG", "TURSG", "UFUK", "ULKER",
    "ULUUN", "UMPAS", "UNLU", "USAK", "VAKBN", "VAKFN", "VAKKO",
    "VANGD", "VBTYZ", "VERUS", "VESBE", "VKGYO", "VKING", "VRGYO", "YBTAS",
    "YGGYO", "YKBNK", "YKSLN", "YONGA", "YYAPI", "ZEDUR", "ZRGYO",
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


def macd_hesapla(seri, hizli=12, yavas=26, sinyal=9):
    ema_h = seri.ewm(span=hizli, adjust=False).mean()
    ema_y = seri.ewm(span=yavas, adjust=False).mean()
    macd = ema_h - ema_y
    sinyal_hat = macd.ewm(span=sinyal, adjust=False).mean()
    return macd.round(2), sinyal_hat.round(2), (macd - sinyal_hat).round(2)


def bollinger_hesapla(seri, periyot=20, std_katsayi=2):
    orta = seri.rolling(periyot).mean()
    std = seri.rolling(periyot).std()
    ust = (orta + std_katsayi * std).round(2)
    alt = (orta - std_katsayi * std).round(2)
    b_yuzde = ((seri - alt) / (ust - alt)).round(4)
    return ust, orta.round(2), alt, b_yuzde


def sinyal_uret(son):
    puanlar = 0
    rsi = son.get("RSI", 50)
    macd_hist = son.get("MACD_Hist", 0)
    b_yuzde = son.get("BB_Yuzde", 0.5)
    ema9 = son.get("EMA9", 0)
    ema21 = son.get("EMA21", 0)
    if rsi < 30: puanlar += 2
    elif rsi < 40: puanlar += 1
    elif rsi > 70: puanlar -= 2
    elif rsi > 60: puanlar -= 1
    if macd_hist > 0: puanlar += 1
    elif macd_hist < 0: puanlar -= 1
    if b_yuzde < 0.1: puanlar += 2
    elif b_yuzde > 0.9: puanlar -= 2
    if ema9 > 0 and ema21 > 0:
        if ema9 > ema21: puanlar += 1
        else: puanlar -= 1
    if puanlar >= 3: return "GUCLU AL"
    elif puanlar >= 1: return "AL"
    elif puanlar <= -3: return "GUCLU SAT"
    elif puanlar <= -1: return "SAT"
    else: return "BEKLE"


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
        print(f"  HATA endeks {ad}: {e}")
        return pd.DataFrame()
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
                "Hacim": int(float(row.get("Volume", 0))), "Degisim": 0.0})
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
        print(f"  HATA {hisse}: {e}")
        return pd.DataFrame()
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
                "Hacim": int(float(row.get("Volume", 0))), "Degisim": 0.0})
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
    print(f"  OK {hisse}: RSI={result.iloc[-1]['RSI']:.1f} MACD={result.iloc[-1]['MACD_Hist']:+.2f} Kapanis={result.iloc[-1]['Kapanis']} TL")
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

    # ANA TABLO — buyuk, tum veri
    sayfa_hazirla(svc, SPREADSHEET_ID,
                  ["Tum Veri", "Ozet", "Sinyaller"] +
                  [f"Endeks_{ad}" for ad in df_endeks_dict])

    for ad, df_e in df_endeks_dict.items():
        if df_e.empty: continue
        cols = list(df_e.columns)
        rows = [[str(v) for v in r] for r in df_e.values.tolist()]
        yaz(svc, SPREADSHEET_ID, f"Endeks_{ad}",
            [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows)
        son = df_e.iloc[-1]
        print(f"  Endeks_{ad}: {son['Kapanis']:,.2f} ({son['Degisim']:+.2f}%)")

    cols = list(df_hisse.columns)
    rows_all = [[str(v) for v in r] for r in df_hisse.values.tolist()]
    yaz(svc, SPREADSHEET_ID, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows_all)
    print(f"  Tum Veri: {len(rows_all)} satir")

    ozet_cols = (["Hisse", "Kapanis", "Degisim"] +
                 [f"EMA{p}" for p in EMA_PERIYOTLARI] +
                 ["RSI","MACD","MACD_Sinyal","MACD_Hist",
                  "BB_Ust","BB_Orta","BB_Alt","BB_Yuzde","SINYAL","Guncelleme"])
    ozet = [ozet_cols]
    for h in sorted(df_hisse["Hisse"].unique().tolist()):
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty: continue
        son = df_h.iloc[-1]
        sinyal = sinyal_uret(son)
        satir = [h, str(son["Kapanis"]), str(son["Degisim"])]
        for p in EMA_PERIYOTLARI: satir.append(str(son.get(f"EMA{p}", "")))
        satir += [str(son.get("RSI","")), str(son.get("MACD","")),
                  str(son.get("MACD_Sinyal","")), str(son.get("MACD_Hist","")),
                  str(son.get("BB_Ust","")), str(son.get("BB_Orta","")),
                  str(son.get("BB_Alt","")), str(son.get("BB_Yuzde","")),
                  sinyal, now]
        ozet.append(satir)
    yaz(svc, SPREADSHEET_ID, "Ozet", ozet)
    print(f"  Ozet: {len(ozet)-1} hisse")

    sin_cols = ["Hisse","Kapanis","Degisim","RSI","MACD_Hist","BB_Yuzde","EMA9","EMA21","SINYAL","Guncelleme"]
    sinyaller = [sin_cols]
    for h in sorted(df_hisse["Hisse"].unique().tolist()):
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty: continue
        son = df_h.iloc[-1]
        sinyaller.append([h, str(son["Kapanis"]), str(son["Degisim"]),
            str(son.get("RSI","")), str(son.get("MACD_Hist","")),
            str(son.get("BB_Yuzde","")), str(son.get("EMA9","")),
            str(son.get("EMA21","")), sinyal_uret(son), now])
    yaz(svc, SPREADSHEET_ID, "Sinyaller", sinyaller)
    al = sum(1 for r in sinyaller[1:] if "AL" in r[-2])
    sat = sum(1 for r in sinyaller[1:] if "SAT" in r[-2])
    print(f"  Sinyaller: {al} AL | {sat} SAT | {len(sinyaller)-1-al-sat} BEKLE")

    # SINYAL TABLOSU — kucuk, NotebookLM icin
    if SINYAL_SPREADSHEET_ID:
        print("\n  NotebookLM sinyal tablosu yaziliyor...")
        sayfa_hazirla(svc, SINYAL_SPREADSHEET_ID,
                      ["Sinyaller", "Ozet", "Endeks"])

        yaz(svc, SINYAL_SPREADSHEET_ID, "Sinyaller", sinyaller)

        ozet_nm_cols = ["Hisse","Kapanis","Degisim","RSI","MACD_Hist",
                        "BB_Yuzde","EMA9","EMA21","EMA50","EMA200",
                        "SINYAL","Yuksek_180g","Dusuk_180g","Guncelleme"]
        ozet_nm = [ozet_nm_cols]
        for h in sorted(df_hisse["Hisse"].unique().tolist()):
            df_h = df_hisse[df_hisse["Hisse"] == h]
            if df_h.empty: continue
            son = df_h.iloc[-1]
            ozet_nm.append([
                h, str(son["Kapanis"]), str(son["Degisim"]),
                str(son.get("RSI","")), str(son.get("MACD_Hist","")),
                str(son.get("BB_Yuzde","")), str(son.get("EMA9","")),
                str(son.get("EMA21","")), str(son.get("EMA50","")),
                str(son.get("EMA200","")), sinyal_uret(son),
                str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), now
            ])
        yaz(svc, SINYAL_SPREADSHEET_ID, "Ozet", ozet_nm)

        endeks_rows = [["Endeks","Son Puan","Degisim%","180g Yuksek","180g Dusuk","Guncelleme"]]
        for ad, df_e in df_endeks_dict.items():
            if df_e.empty: continue
            son = df_e.iloc[-1]
            endeks_rows.append([ad, str(son["Kapanis"]), str(son["Degisim"]),
                str(df_e["Yuksek"].max()), str(df_e["Dusuk"].min()), now])
        yaz(svc, SINYAL_SPREADSHEET_ID, "Endeks", endeks_rows)
        print(f"  NotebookLM tablosu OK — {len(ozet_nm)-1} hisse, {len(endeks_rows)-1} endeks")


def main():
    print("="*50)
    print(f"BIST Katilim - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
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
