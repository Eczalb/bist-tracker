import os, json, time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────
GECMIS_GUN = 180        # Kac gunluk veri tutulsun
TIMEOUT_SANIYE = 15

# XK100 — BIST Katilim 100 resmi listesi (Ekim 2025 - Nisan 2026)
HISSELER = [
    "AKFYE", "AKSA", "ALBRK", "ALCTL", "ALTNY", "ALVES", "ARDYZ",
    "ASELS", "ATATP", "AVPGY", "BANVT", "BEGYO", "BERA", "BIENY",
    "BIMAS", "BINBN", "BINHO", "BMSTL", "BSOKE", "CANTE", "CEMTS",
    "CIMSA", "CMBTN", "CVKMD", "CWENE", "DAPGM", "DCTTR", "DYOBY",
    "EGGUB", "EKGYO", "ELITE", "ENJSA", "EREGL", "EUPWR", "FONET",
    "FORMT", "FZLGY", "GENIL", "GENTS", "GEREL", "GESAN", "GOODY",
    "GOKNR", "GUBRF", "GLRMK", "GRSEL", "HRKET", "IHEVA", "IHLGM",
    "IHLAS", "IHYAY", "IMASM", "INTEM", "JANTS", "KATMR", "KCAER",
    "KLMSN", "KNFRT", "KONYA", "KOPOL", "KRSTL", "LOGO", "MAVI",
    "MNDRS", "MIATK", "MPARK", "NUHCM", "ODAS", "ORGE", "OTKAR",
    "OYAKC", "PARSN", "PATEK", "PGSUS", "PRDBC", "RYSAS", "SANEL",
    "SASA", "SELEC", "SILVR", "SISE", "SOKM", "SONME", "TABGD",
    "TAVHL", "THYAO", "TOASO", "TRCAS", "TRILC", "TTRAK", "TUPRS",
    "ULKER", "UMPAS", "VESBE", "VKGYO", "YBTAS", "YONGA", "ZEDUR",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
# ─────────────────────────────────────────


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


def hisse_cek(hisse, gun=180):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 10)
    try:
        ham = yf.download(sembol, start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"), interval="1d",
                          progress=False, auto_adjust=True, timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA {hisse}: {e}"); return pd.DataFrame()
    if ham is None or ham.empty: return pd.DataFrame()
    ham = multiindex_duzelt(ham, sembol)
    if any(s not in ham.columns for s in ["Open", "Close", "Volume"]):
        return pd.DataFrame()
    df = ham.tail(gun).copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append({
                "Hisse": str(hisse),
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row["Open"]), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
            })
        except: continue
    if not rows: return pd.DataFrame()
    result = pd.DataFrame(rows)
    print(f"  OK {hisse}: {len(result)} gun, son kapanis: {result.iloc[-1]['Kapanis']} TL")
    return result


def xu100_cek(gun=180):
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 10)
    try:
        ham = yf.download("XU100.IS", start=baslangic.strftime("%Y-%m-%d"),
                          end=bitis.strftime("%Y-%m-%d"), interval="1d",
                          progress=False, auto_adjust=True, timeout=TIMEOUT_SANIYE)
    except Exception as e:
        print(f"  HATA XU100: {e}"); return pd.DataFrame()
    if ham is None or ham.empty: return pd.DataFrame()
    ham = multiindex_duzelt(ham, "XU100.IS")
    if "Close" not in ham.columns: return pd.DataFrame()
    df = ham.tail(gun).copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append({
                "Hisse": "XU100",
                "Tarih": pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                "Acilis": round(float(row.get("Open", 0)), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
            })
        except: continue
    if not rows: return pd.DataFrame()
    result = pd.DataFrame(rows)
    print(f"  OK XU100: son puan {result.iloc[-1]['Kapanis']:,.2f}")
    return result


def guncelle(df):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json: raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    svc = build("sheets", "v4", credentials=Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Mevcut sayfalari kontrol et
    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # "Veriler" sayfasi yoksa ilk sayfayi yeniden adlandir
    if "Veriler" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": "Veriler"},
                "fields": "title"}}]}).execute()
        time.sleep(1)

    # Tum sayfayi TEMIZLE (eski veriyi sil)
    svc.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range="Veriler"
    ).execute()

    # Yeni veriyi yaz
    cols = list(df.columns)
    rows = [[str(v) for v in r] for r in df.values.tolist()]
    baslik = [[f"Son guncelleme: {now} | {len(df['Hisse'].unique())-1} hisse + XU100 | {GECMIS_GUN} gun"] + [""] * (len(cols)-1)]

    svc.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Veriler!A1",
        valueInputOption="RAW",
        body={"values": baslik + [cols] + rows}
    ).execute()

    hisse_sayisi = len([h for h in df["Hisse"].unique() if h != "XU100"])
    print(f"  Veriler sayfasi guncellendi: {len(rows)} satir ({hisse_sayisi} hisse + XU100)")


def main():
    print("=" * 50)
    print(f"XK100 Veri Guncelleme - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Hisse: {len(HISSELER)} | Son {GECMIS_GUN} gun")
    print("=" * 50)

    tum = []

    # XU100 endeksi
    print("\nXU100 cekiliyor...")
    df_xu = xu100_cek(GECMIS_GUN)
    if not df_xu.empty:
        tum.append(df_xu)
    time.sleep(1)

    # Katilim hisseleri
    print("\nHisseler cekiliyor...")
    hata = []
    for i, h in enumerate(HISSELER, 1):
        print(f"[{i}/{len(HISSELER)}] {h}...")
        df = hisse_cek(h, GECMIS_GUN)
        if not df.empty:
            tum.append(df)
        else:
            hata.append(h)
        time.sleep(0.3)

    print(f"\nBasarili: {len(tum)-1} hisse + XU100 | Hata: {len(hata)}")
    if hata: print(f"Atlananlar: {hata}")
    if not tum: raise SystemExit("Hic veri yok!")

    # Birlestir: once XU100, sonra hisseler (tarihe gore sirali)
    df_tum = pd.concat(tum, ignore_index=True).sort_values(["Hisse", "Tarih"])
    print(f"Toplam: {len(df_tum)} satir")

    guncelle(df_tum)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
