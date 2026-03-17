"""
BIST Hisse Takip Sistemi
Gunluk verileri yfinance'den ceker ve Google Sheets'e yazar.
"""

import os
import json
import yfinance as yf
import pandas as pd
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
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")


def bist_sembol(hisse):
    return f"{hisse}.IS"


def veri_cek(hisse, gun=30):
    sembol = bist_sembol(hisse)
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 10)
    ticker = yf.Ticker(sembol)
    df = ticker.history(start=baslangic.strftime("%Y-%m-%d"),
                        end=bitis.strftime("%Y-%m-%d"),
                        interval="1d")
    if df.empty:
        print(f"  {hisse} icin veri bulunamadi.")
        return pd.DataFrame()
    df = df.tail(gun).copy()
    df.index = df.index.tz_localize(None)
    df = df.reset_index()
    df["Hisse"] = hisse
    df["Tarih"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Degisim"] = ((df["Close"] - df["Open"]) / df["Open"] * 100).round(2)
    df = df[["Hisse", "Tarih", "Open", "High", "Low", "Close", "Volume", "Degisim"]]
    df.columns = ["Hisse", "Tarih", "Acilis", "Yuksek", "Dusuk", "Kapanis", "Hacim", "Degisim %"]
    for col in ["Acilis", "Yuksek", "Dusuk", "Kapanis"]:
        df[col] = df[col].round(2)
    return df


def google_sheets_guncelle(df_tumu):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON bulunamadi!")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    baslık = [df_tumu.columns.tolist()]
    satirlar = [[str(v) for v in row] for row in df_tumu.values.tolist()]
    guncelleme_zamani = datetime.now().strftime("%d.%m.%Y %H:%M")
    bilgi = [[f"Son guncelleme: {guncelleme_zamani} (UTC)", "", "", "", "", "", "", ""]]

    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="Tum Veri!A1",
        valueInputOption="RAW", body={"values": bilgi + baslık + satirlar}).execute()
    print(f"  Tum Veri guncellendi ({len(satirlar)} satir).")

    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    istekler = []
    for hisse in df_tumu["Hisse"].unique():
        if hisse not in mevcut:
            istekler.append({"addSheet": {"properties": {"title": hisse}}})
    if "Ozet" not in mevcut:
        istekler.append({"addSheet": {"properties": {"title": "Ozet"}}})
    if istekler:
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": istekler}).execute()

    for hisse in df_tumu["Hisse"].unique():
        df_h = df_tumu[df_tumu["Hisse"] == hisse].copy()
        veri = [df_h.columns.tolist()] + [[str(v) for v in row] for row in df_h.values.tolist()]
        sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=f"{hisse}!A1",
            valueInputOption="RAW", body={"values": veri}).execute()
        print(f"  {hisse}: {df_h.iloc[-1]['Kapanis']} TL")

    ozet = [["Hisse", "Son Kapanis", "Degisim %", "30G Yuksek", "30G Dusuk", "Guncelleme"]]
    for hisse in df_tumu["Hisse"].unique():
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        son = df_h.iloc[-1]
        ozet.append([hisse, str(son["Kapanis"]), str(son["Degisim %"]),
                     str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), guncelleme_zamani])
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="Ozet!A1",
        valueInputOption="RAW", body={"values": ozet}).execute()
    print("  Ozet guncellendi.")


def main():
    print("BIST Veri Guncelleme")
    tum_df = []
    for hisse in HISSELER:
        print(f"{hisse} cekiliyor...")
        df = veri_cek(hisse, GECMIS_GUN)
        if not df.empty:
            tum_df.append(df)
    if not tum_df:
        print("Hic veri cekilemedi!")
        return
    df_tumu = pd.concat(tum_df, ignore_index=True).sort_values(["Hisse", "Tarih"])
    google_sheets_guncelle(df_tumu)
    print("Tamamlandi!")


if __name__ == "__main__":
    main()
