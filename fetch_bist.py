import os, json, time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# AYARLAR — istedigin zaman degistir
# ─────────────────────────────────────────

# Kac gunluk veri cekilsin? (ornek: 30, 90, 180)
GECMIS_GUN = 180

# EMA periyotlari (bos birak = hesaplama)
EMA_PERIYOTLARI = [9, 21, 50, 200]

# Tum BIST Katilim Endeksi hisseleri
HISSELER = [
    "AEFES", "AGESA", "AKBNK", "AKCNS", "AKFGY", "AKGRT", "AKSA", "AKSEN",
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
    "ULUUN", "UMPAS", "UNLU", "USAK", "USDRAM", "VAKBN", "VAKFN", "VAKKO",
    "VANGD", "VBTYZ", "VERUS", "VESBE", "VKGYO", "VKING", "VRGYO", "YBTAS",
    "YGGYO", "YKBNK", "YKSLN", "YONGA", "YYAPI", "ZEDUR", "ZRGYO",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()

# ─────────────────────────────────────────


def ema_hesapla(df, periyot):
    """Pandas ile EMA hesapla."""
    return df["Kapanis"].ewm(span=periyot, adjust=False).round(2)


def veri_cek(hisse, gun=30):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 50)
    try:
        ham = yf.download(
            sembol,
            start=baslangic.strftime("%Y-%m-%d"),
            end=bitis.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        print(f"  HATA {hisse}: {e}")
        return pd.DataFrame()

    if ham is None or ham.empty:
        return pd.DataFrame()

    if isinstance(ham.columns, pd.MultiIndex):
        level0 = ham.columns.get_level_values(0).tolist()
        level1 = ham.columns.get_level_values(1).tolist()
        if sembol in level0:
            ham = ham.xs(sembol, axis=1, level=0)
        elif sembol in level1:
            ham = ham.xs(sembol, axis=1, level=1)
        elif "Close" in level0:
            ham.columns = ham.columns.droplevel(1)
        else:
            ham.columns = ham.columns.droplevel(0)

    gerekli = ["Open", "High", "Low", "Close", "Volume"]
    if any(s not in ham.columns for s in gerekli):
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

    # Gunluk degisim
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round((result.at[i, "Kapanis"] - prev) / prev * 100, 2)

    # EMA hesapla
    for p in EMA_PERIYOTLARI:
        result[f"EMA{p}"] = ema_hesapla(result, p)

    # Son N gunu al
    result = result.tail(gun).reset_index(drop=True)
    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result


def yaz(svc, sid, sayfa, veri):
    svc.values().update(
        spreadsheetId=sid, range=f"'{sayfa}'!A1",
        valueInputOption="RAW", body={"values": veri}
    ).execute()


def sayfa_ekle(svc, sid, baslik):
    svc.batchUpdate(spreadsheetId=sid,
        body={"requests": [{"addSheet": {"properties": {"title": str(baslik)}}}]}
    ).execute()


def guncelle(df):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    svc = build("sheets", "v4", credentials=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # Tum Veri sayfasi
    if "Tum Veri" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": "Tum Veri"},
                "fields": "title"}}
        ]}).execute()
        mevcut[0] = "Tum Veri"

    cols = list(df.columns)
    rows_all = [[str(v) for v in r] for r in df.values.tolist()]
    yaz(svc, SPREADSHEET_ID, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows_all)
    print(f"  Tum Veri: {len(rows_all)} satir")

    # Her hisse icin ayri sayfa
    basarili_hisseler = df["Hisse"].unique().tolist()
    for h in basarili_hisseler:
        df_h = df[df["Hisse"] == h]
        if df_h.empty:
            continue
        if h not in mevcut:
            sayfa_ekle(svc, SPREADSHEET_ID, h)
        yaz(svc, SPREADSHEET_ID, h,
            [cols] + [[str(v) for v in r] for r in df_h.values.tolist()])

    # Ozet sayfasi
    if "Ozet" not in mevcut:
        sayfa_ekle(svc, SPREADSHEET_ID, "Ozet")

    ozet_cols = ["Hisse", "Son Kapanis", "Degisim %", "Yuksek", "Dusuk"] +                 [f"EMA{p}" for p in EMA_PERIYOTLARI] + ["Guncelleme"]
    ozet = [ozet_cols]
    for h in basarili_hisseler:
        df_h = df[df["Hisse"] == h]
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
    print(f"  Ozet: {len(ozet)-1} hisse")


def main():
    print("="*50)
    print(f"BIST Katilim - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Hisse sayisi: {len(HISSELER)} | Periyot: {GECMIS_GUN} gun")
    print("="*50)

    tum = []
    hata = []
    for i, h in enumerate(HISSELER, 1):
        print(f"[{i}/{len(HISSELER)}] {h}...")
        df = veri_cek(h, GECMIS_GUN)
        if not df.empty:
            tum.append(df)
        else:
            hata.append(h)
        time.sleep(0.5)  # Rate limit

    print(f"\nBasarili: {len(tum)} | Bos/Hata: {len(hata)}")
    if hata:
        print(f"Bos gelenler: {hata}")

    if not tum:
        raise SystemExit("Hic veri yok!")

    df_tum = pd.concat(tum, ignore_index=True).sort_values(["Hisse","Tarih"])
    print(f"Toplam: {len(df_tum)} satir")
    guncelle(df_tum)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
