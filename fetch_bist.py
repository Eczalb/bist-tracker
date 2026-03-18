import os, json, time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# AYARLAR — istedigin zaman degistir
# ─────────────────────────────────────────
GECMIS_GUN = 180
EMA_PERIYOTLARI = [9, 21, 50, 200]
TIMEOUT_SANIYE = 15  # Her hisse icin max bekleme

ENDEKSLER = {
    "XU100": "XU100.IS",
    "XKTUM": "XKTUM.IS",
}

# Sadece BIST Katilim hisseleri
HISSELER = [
    "AEFES", "AGESA", "AKCNS", "AKFGY", "AKGRT", "AKSA", "AKSEN",
    "ALARK", "ALBRK", "ALFAS", "ALGYO", "ALKIM", "ALTIN", "ANSGR", "ARCLK",
    "ARDYZ", "ARSAN", "ASELS", "ASGYO", "ASTOR", "ATAKP", "ATATP", "ATGYO",
    "AVGYO", "AYCES", "AYEN", "AYGAZ", "AZTEK", "BAGFS", "BAKAB", "BANVT",
    "BERA", "BFREN", "BIENY", "BIGCH", "BIMAS", "BIOEN", "BIZIM", "BMSTL",
    "BNTAS", "BOSSA", "BOYP", "BRKO", "BRMEN", "BRKVY", "BRSAN", "BRYAT",
    "BSOKE", "BTCIM", "BUCIM", "BURCE", "BURVA", "BVSAN", "CCOLA", "CELHA",
    "CEMAS", "CEMTS", "CIMSA", "CLEBI", "CMENT", "CMSAS", "CRFSA", "CUSAN",
    "CWENE", "DARDL", "DENGE", "DERHL", "DESA", "DEVA", "DGATE", "DGKLB",
    "DGNMO", "DITAS", "DMSAS", "DNISI", "DOAS", "DOBUR", "DOCO", "DOGUB",
    "DOHOL", "DURAN", "ECILC", "ECZYT", "EDATA", "EGEEN", "EGPRO", "EGSER",
    "EKGYO", "EMKEL", "EMNIS", "ENERY", "ENJSA", "ENKAI", "EPLAS", "ERBOS",
    "ERCB", "EREGL", "ERSU", "ESCAR", "ESCOM", "ESEN", "ETILR", "EUHOL",
    "EUPWR", "EYGYO", "FENER", "FLAP", "FMIZP", "FONET", "FORMT", "FRIGO",
    "FROTO", "FZLGY", "GARAN", "GARFA", "GEDZA", "GEREL", "GLBMD", "GLCVY",
    "GLYHO", "GMTAS", "GOODY", "GOZDE", "GRSEL", "GSDHO", "GSRAY", "GUBRF",
    "GWIND", "GYSIS", "HALKB", "HATEK", "HDFGS", "HEDEF", "HILAL", "HLGYO",
    "HTTBT", "HUBVC", "HUNER", "HURGZ", "ICBCT", "ICUGS", "IDEAS", "IEYHO",
    "IHEVA", "IHGZT", "IHLAS", "IHLGM", "IHYAY", "IMASM", "INFO", "INTEM",
    "INVEO", "IPEKE", "ISGYO", "ISSEN", "ISYAT", "ITTFK", "IZFAS", "IZOCM",
    "JANTS", "KAPLM", "KAREL", "KARSN", "KARTN", "KATMR", "KAYSE", "KCAER",
    "KCHOL", "KENT", "KERVN", "KERVT", "KFEIN", "KGYO", "KIMMR", "KLGYO",
    "KLKIM", "KLMSN", "KLRHO", "KLSER", "KMPUR", "KNFRT", "KONYA", "KOPOL",
    "KORDS", "KOZAA", "KOZAL", "KRDMA", "KRDMB", "KRDMD", "KRGYO", "KRONT",
    "KRPLS", "KRSTL", "KRTEK", "KSTUR", "KTLEV", "KUTPO", "LIDER", "LIDFA",
    "LKMNH", "LOGO", "LRSHO", "LUKSK", "MAALT", "MACKO", "MAGEN", "MAKIM",
    "MAKTK", "MANAS", "MARKA", "MEDTR", "MEGAP", "MEKAG", "MEPET", "MERCN",
    "MERIT", "MERKO", "METRO", "METUR", "MGROS", "MIATK", "MIGRS", "MMCAS",
    "MNDRS", "MNVRO", "MOBTL", "MPARK", "MRGYO", "MRSHL", "MSGYO", "MTRKS",
    "MZHLD", "NATEN", "NETAS", "NIBAS", "NTHOL", "NTTUR", "NUGYO", "NUHCM",
    "OBASE", "ODAS", "ONCSM", "ONRYT", "ORCAY", "ORGE", "ORMA", "OSTIM",
    "OTKAR", "OYAKC", "OYLUM", "OYYAT", "OZGYO", "OZKGY", "PAMEL", "PAPIL",
    "PARSN", "PASEU", "PATEK", "PCILT", "PEGYO", "PEKGY", "PGSUS", "PKART",
    "PNLSN", "POLHO", "POLTK", "PRDBC", "PRZMA", "PSDTC", "PSGYO", "QNBFL",
    "QNBFB", "RAYSG", "RHEAG", "RNPOL", "RODRG", "ROYAL", "RTALB", "RUBNS",
    "RYGYO", "RYSAS", "SAFKR", "SAHOL", "SAMAT", "SANEL", "SANFM", "SANKO",
    "SARKY", "SASA", "SAYAS", "SDTTR", "SELEC", "SELGD", "SEYKM", "SILVR",
    "SISE", "SKBNK", "SMRTG", "SNGYO", "SNKRN", "SODSN", "SOKM", "SONME",
    "SRVGY", "SUMAS", "SUWEN", "TABGD", "TARKM", "TATGD", "TAVHL", "TBORG",
    "TCELL", "TDGYO", "TEKTU", "TETMT", "THYAO", "TKNSA", "TKURU", "TLMAN",
    "TMPOL", "TOASO", "TRCAS", "TRGYO", "TRILC", "TSKB", "TSPOR", "TTKOM",
    "TTRAK", "TUCLK", "TUPRS", "TUREX", "TURGG", "TURSG", "UFUK", "ULKER",
    "ULUUN", "UMPAS", "UNLU", "USAK", "VAKBN", "VAKFN", "VAKKO",
    "VANGD", "VBTYZ", "VERUS", "VESBE", "VKGYO", "VKING", "VRGYO", "YBTAS",
    "YGGYO", "YKBNK", "YKSLN", "YONGA", "YYAPI", "ZEDUR", "ZRGYO",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
# ─────────────────────────────────────────


def multiindex_duzelt(ham, sembol):
    if not isinstance(ham.columns, pd.MultiIndex):
        return ham
    l0 = ham.columns.get_level_values(0).tolist()
    l1 = ham.columns.get_level_values(1).tolist()
    if sembol in l0:
        return ham.xs(sembol, axis=1, level=0)
    elif sembol in l1:
        return ham.xs(sembol, axis=1, level=1)
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
        ham = yf.download(sembol,
                          start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True,
                          timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA endeks {ad}: {e}")
        return pd.DataFrame()
    if ham is None or ham.empty:
        return pd.DataFrame()
    ham = multiindex_duzelt(ham, sembol)
    if "Close" not in ham.columns:
        return pd.DataFrame()
    df = ham.tail(gun).copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append({
                "Endeks": str(ad),
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row.get("Open", 0)), 2),
                "Yuksek": round(float(row.get("High", 0)), 2),
                "Dusuk": round(float(row.get("Low", 0)), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
                "Degisim": 0.0,
            })
        except:
            continue
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round(
                (result.at[i, "Kapanis"] - prev) / prev * 100, 2)
    print(f"  OK {ad}: son puan {result.iloc[-1]['Kapanis']:,.2f}")
    return result


def veri_cek(hisse, gun=180):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 50)
    try:
        ham = yf.download(sembol,
                          start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"),
                          interval="1d", progress=False, auto_adjust=True,
                          timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA {hisse}: {e}")
        return pd.DataFrame()
    if ham is None or ham.empty:
        return pd.DataFrame()
    ham = multiindex_duzelt(ham, sembol)
    if any(s not in ham.columns for s in ["Open", "High", "Low", "Close", "Volume"]):
        return pd.DataFrame()
    df = ham.tail(gun + 50).copy()
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
        except:
            continue
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round(
                (result.at[i, "Kapanis"] - prev) / prev * 100, 2)
    for p in EMA_PERIYOTLARI:
        result[f"EMA{p}"] = result["Kapanis"].ewm(span=p, adjust=False).mean().round(2)
    result = result.tail(gun).reset_index(drop=True)
    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result


def yaz(svc, sid, sayfa, veri):
    svc.values().update(spreadsheetId=sid, range=f"'{sayfa}'!A1",
        valueInputOption="RAW", body={"values": veri}).execute()


def sayfa_ekle(svc, sid, baslik):
    svc.batchUpdate(spreadsheetId=sid,
        body={"requests": [{"addSheet": {"properties": {"title": str(baslik)}}}]}).execute()


def guncelle(df_hisse, df_endeks_dict):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    svc = build("sheets", "v4", credentials=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # Endeks sayfalari
    for ad, df_e in df_endeks_dict.items():
        if df_e.empty:
            continue
        sayfa_adi = f"Endeks_{ad}"
        if sayfa_adi not in mevcut:
            sayfa_ekle(svc, SPREADSHEET_ID, sayfa_adi)
        cols = list(df_e.columns)
        rows = [[str(v) for v in r] for r in df_e.values.tolist()]
        yaz(svc, SPREADSHEET_ID, sayfa_adi,
            [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows)
        son = df_e.iloc[-1]
        print(f"  {sayfa_adi}: {son['Kapanis']:,.2f} ({son['Degisim']:+.2f}%)")

    # Tum Veri
    if "Tum Veri" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": "Tum Veri"},
                "fields": "title"}}]}).execute()
        mevcut[0] = "Tum Veri"
    cols = list(df_hisse.columns)
    rows_all = [[str(v) for v in r] for r in df_hisse.values.tolist()]
    yaz(svc, SPREADSHEET_ID, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows_all)
    print(f"  Tum Veri: {len(rows_all)} satir")

    # Her hisse ayri sayfa
    for h in df_hisse["Hisse"].unique().tolist():
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty:
            continue
        if h not in mevcut:
            sayfa_ekle(svc, SPREADSHEET_ID, h)
        yaz(svc, SPREADSHEET_ID, h,
            [cols] + [[str(v) for v in r] for r in df_h.values.tolist()])

    # Ozet
    if "Ozet" not in mevcut:
        sayfa_ekle(svc, SPREADSHEET_ID, "Ozet")
    ozet_cols = (["Hisse/Endeks", "Son Kapanis", "Degisim %", "Yuksek", "Dusuk"] +
                 [f"EMA{p}" for p in EMA_PERIYOTLARI] + ["Guncelleme"])
    ozet = [ozet_cols]
    for ad, df_e in df_endeks_dict.items():
        if df_e.empty:
            continue
        son = df_e.iloc[-1]
        satir = [f"[ENDEKS] {ad}", str(son["Kapanis"]), str(son["Degisim"]),
                 str(df_e["Yuksek"].max()), str(df_e["Dusuk"].min())]
        satir += [""] * len(EMA_PERIYOTLARI)
        satir.append(now)
        ozet.append(satir)
    for h in df_hisse["Hisse"].unique().tolist():
        df_h = df_hisse[df_hisse["Hisse"] == h]
        if df_h.empty:
            continue
        son = df_h.iloc[-1]
        satir = [h, str(son["Kapanis"]), str(son["Degisim"]),
                 str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min())]
        for p in EMA_PERIYOTLARI:
            satir.append(str(son.get(f"EMA{p}", "")))
        satir.append(now)
        ozet.append(satir)
    yaz(svc, SPREADSHEET_ID, "Ozet", ozet)
    print(f"  Ozet: {len(ozet)-1} satir")


def main():
    print("="*50)
    print(f"BIST Katilim Takip - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Hisse: {len(HISSELER)} | Endeks: {len(ENDEKSLER)} | Periyot: {GECMIS_GUN} gun")
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
        if not df.empty:
            tum.append(df)
        else:
            hata.append(h)
        time.sleep(0.3)

    print(f"\nBasarili: {len(tum)} | Hata: {len(hata)}")
    if hata:
        print(f"Atlananlar: {hata}")
    if not tum:
        raise SystemExit("Hic veri yok!")

    df_hisse = pd.concat(tum, ignore_index=True).sort_values(["Hisse", "Tarih"])
    print(f"Toplam: {len(df_hisse)} satir")
    guncelle(df_hisse, df_endeks_dict)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
