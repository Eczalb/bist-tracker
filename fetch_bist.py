import os
import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

HISSELER = ["THYAO", "ASELS", "EREGL", "SISE", "KCHOL"]
GECMIS_GUN = 30
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()


def veri_cek(hisse, gun=30):
    sembol = f"{hisse}.IS"
    bitis = datetime.today()
    baslangic = bitis - timedelta(days=gun + 20)

    try:
        ham = yf.download(
            sembol,
            start=baslangic.strftime("%Y-%m-%d"),
            end=bitis.strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=True,
            group_by="ticker",
        )
    except Exception as e:
        print(f"  HATA {hisse} indirme: {e}")
        return pd.DataFrame()

    if ham is None or ham.empty:
        print(f"  {hisse}: bos geldi")
        return pd.DataFrame()

    # MultiIndex varsa tek seviyeye indir
    if isinstance(ham.columns, pd.MultiIndex):
        # group_by=ticker oldugunda (THYAO.IS, Open) seklinde gelir
        # Sadece ilk seviyeyi at
        ham.columns = ham.columns.droplevel(1) if ham.columns.nlevels > 1 else ham.columns

    # Gerekli sutunlar var mi kontrol et
    gerekli = ["Open", "High", "Low", "Close", "Volume"]
    eksik = [s for s in gerekli if s not in ham.columns]
    if eksik:
        print(f"  {hisse}: eksik sutun {eksik}, mevcut: {list(ham.columns)}")
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
                "Yuksek": round(float(row["High"]), 2),
                "Dusuk": round(float(row["Low"]), 2),
                "Kapanis": round(float(row["Close"]), 2),
                "Hacim": int(float(row.get("Volume", 0))),
                "Degisim %": 0.0,
            })
        except Exception:
            continue

    if not rows:
        print(f"  {hisse}: satirlar islenmedi")
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    # Gunluk degisim hesapla
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        curr = result.at[i, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim %"] = round((curr - prev) / prev * 100, 2)

    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result


def sheets_yaz(sheet, spreadsheet_id, sayfa, veri):
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sayfa}!A1",
        valueInputOption="RAW",
        body={"values": veri}
    ).execute()


def sayfa_olustur(sheet, spreadsheet_id, baslik):
    sheet.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": baslik}}}]}
    ).execute()


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

    # Mevcut sayfalari al
    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]
    print(f"  Mevcut sayfalar: {mevcut}")

    # Tum Veri sayfasi
    if "Tum Veri" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
            "requests": [{"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": "Tum Veri"},
                "fields": "title"
            }}]
        }).execute()
        mevcut[0] = "Tum Veri"
        print("  'Tum Veri' sayfasi olusturuldu")

    baslık = [list(df_tumu.columns)]
    satirlar = [[str(v) for v in row] for row in df_tumu.values.tolist()]
    bilgi = [[f"Son guncelleme: {guncelleme} UTC"] + [""] * (len(baslık[0]) - 1)]
    sheets_yaz(sheet, SPREADSHEET_ID, "Tum Veri", bilgi + baslık + satirlar)
    print(f"  Tum Veri: {len(satirlar)} satir yazildi")

    # Her hisse icin ayri sayfa
    for hisse in HISSELER:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        if df_h.empty:
            print(f"  {hisse}: veri yok, atlandi")
            continue
        if hisse not in mevcut:
            sayfa_olustur(sheet, SPREADSHEET_ID, hisse)
            print(f"  '{hisse}' sayfasi olusturuldu")
        veri = [list(df_h.columns)] + [[str(v) for v in r] for r in df_h.values.tolist()]
        sheets_yaz(sheet, SPREADSHEET_ID, hisse, veri)
        print(f"  {hisse}: {len(df_h)} satir yazildi")

    # Ozet sayfasi
    if "Ozet" not in mevcut:
        sayfa_olustur(sheet, SPREADSHEET_ID, "Ozet")
    ozet = [["Hisse", "Son Kapanis", "Degisim %", "30G Yuksek", "30G Dusuk", "Guncelleme"]]
    for hisse in HISSELER:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        if df_h.empty:
            continue
        son = df_h.iloc[-1]
        ozet.append([
            hisse,
            str(son["Kapanis"]),
            str(son["Degisim %"]),
            str(df_h["Yuksek"].max()),
            str(df_h["Dusuk"].min()),
            guncelleme
        ])
    sheets_yaz(sheet, SPREADSHEET_ID, "Ozet", ozet)
    print("  Ozet: yazildi")


def main():
    print("=" * 40)
    print(f"BIST - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 40)
    print(f"Spreadsheet ID: {SPREADSHEET_ID[:20]}...")

    tum_df = []
    for hisse in HISSELER:
        print(f"\n{hisse}...")
        df = veri_cek(hisse, GECMIS_GUN)
        if not df.empty:
            tum_df.append(df)
        time.sleep(2)

    if not tum_df:
        print("HATA: Hic veri yok!")
        raise SystemExit(1)

    df_tumu = pd.concat(tum_df, ignore_index=True)
    df_tumu = df_tumu.sort_values(["Hisse", "Tarih"]).reset_index(drop=True)
    print(f"\nToplam {len(df_tumu)} satir")
    print(f"Hisseler: {df_tumu['Hisse'].unique().tolist()}")

    google_sheets_guncelle(df_tumu)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
