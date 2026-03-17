import os
import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

HISSELER = [
    "THYAO",
    "ASELS",
    "EREGL",
    "SISE",
    "KCHOL",
]

GECMIS_GUN = 30
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()


def veri_cek(hisse, gun=30):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 20)

    try:
        df = yf.download(
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

    if df is None or df.empty:
        print(f"  {hisse}: veri bos")
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.tail(gun).copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.reset_index()

    result = pd.DataFrame()
    result["Hisse"] = str(hisse)
    result["Tarih"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    result["Acilis"] = df["Open"].round(2)
    result["Yuksek"] = df["High"].round(2)
    result["Dusuk"] = df["Low"].round(2)
    result["Kapanis"] = df["Close"].round(2)
    result["Hacim"] = df["Volume"].fillna(0).astype(int)
    prev = result["Kapanis"].shift(1)
    result["Degisim %"] = ((result["Kapanis"] - prev) / prev * 100).round(2).fillna(0)

    # NaN temizle
    result = result.dropna(subset=["Hisse", "Tarih", "Kapanis"])
    result["Hisse"] = result["Hisse"].astype(str)

    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result


def google_sheets_guncelle(df_tumu):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON bulunamadi!")

    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    guncelleme = datetime.now().strftime("%d.%m.%Y %H:%M")

    # NaN iceren satirlari temizle
    df_tumu = df_tumu.dropna(subset=["Hisse"])
    df_tumu["Hisse"] = df_tumu["Hisse"].astype(str)
    # Sadece HISSELER listesindeki gercek hisseleri al
    hisse_listesi = [h for h in df_tumu["Hisse"].unique() if str(h) in HISSELER]

    # Mevcut sayfalari al
    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # Tum Veri sayfasi
    hedef_sayfa = "Tum Veri"
    if hedef_sayfa not in mevcut:
        ilk_sayfa_id = meta["sheets"][0]["properties"]["sheetId"]
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
            "requests": [{"updateSheetProperties": {
                "properties": {"sheetId": ilk_sayfa_id, "title": hedef_sayfa},
                "fields": "title"
            }}]
        }).execute()
        mevcut[0] = hedef_sayfa

    baslık = [df_tumu.columns.tolist()]
    satirlar = [[str(v) for v in row] for row in df_tumu.values.tolist()]
    bilgi = [[f"Son guncelleme: {guncelleme} UTC", "", "", "", "", "", "", ""]]
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{hedef_sayfa}!A1",
        valueInputOption="RAW",
        body={"values": bilgi + baslık + satirlar}
    ).execute()
    print(f"  {hedef_sayfa} guncellendi: {len(satirlar)} satir")

    # Eksik sayfalari olustur - sadece gecerli hisse adlari
    istekler = []
    for h in hisse_listesi + ["Ozet"]:
        if str(h) not in mevcut:
            istekler.append({"addSheet": {"properties": {"title": str(h)}}})
    if istekler:
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID,
                          body={"requests": istekler}).execute()

    # Her hisse sayfasini yaz
    for hisse in hisse_listesi:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        veri = [df_h.columns.tolist()] + [[str(v) for v in r] for r in df_h.values.tolist()]
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{hisse}!A1",
            valueInputOption="RAW",
            body={"values": veri}
        ).execute()
        print(f"  {hisse} sayfasi guncellendi")

    # Ozet sayfasi
    ozet = [["Hisse", "Son Kapanis", "Degisim %", "30G En Yuksek", "30G En Dusuk", "Guncelleme"]]
    for hisse in hisse_listesi:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        son = df_h.iloc[-1]
        ozet.append([str(hisse), str(son["Kapanis"]), str(son["Degisim %"]),
                     str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), guncelleme])
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Ozet!A1",
        valueInputOption="RAW",
        body={"values": ozet}
    ).execute()
    print("  Ozet guncellendi")


def main():
    print("=" * 40)
    print(f"BIST Veri Guncelleme - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 40)

    tum_df = []
    for hisse in HISSELER:
        print(f"\n{hisse} cekiliyor...")
        df = veri_cek(hisse, GECMIS_GUN)
        if not df.empty:
            tum_df.append(df)
        time.sleep(1)

    if not tum_df:
        print("Hic veri cekilemedi!")
        return

    df_tumu = pd.concat(tum_df, ignore_index=True).sort_values(["Hisse", "Tarih"])
    print(f"\nToplam {len(df_tumu)} satir, Sheets yaziliyor...")
    google_sheets_guncelle(df_tumu)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
