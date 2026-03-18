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
        )
    except Exception as e:
        print(f"  HATA {hisse}: {e}")
        return pd.DataFrame()

    if ham is None or ham.empty:
        print(f"  {hisse}: bos")
        return pd.DataFrame()

    print(f"  {hisse} ham sutunlar: {list(ham.columns)}")

    # MultiIndex ise ticker seviyesini (0. seviye) kaldir
    if isinstance(ham.columns, pd.MultiIndex):
        ham.columns = ham.columns.droplevel(0)

    print(f"  {hisse} duzeltilmis sutunlar: {list(ham.columns)}")

    gerekli = ["Open", "High", "Low", "Close", "Volume"]
    eksik = [s for s in gerekli if s not in ham.columns]
    if eksik:
        print(f"  {hisse}: hala eksik {eksik}")
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
        except Exception as ex:
            print(f"  satir atlandi: {ex}")
            continue

    if not rows:
        print(f"  {hisse}: hic satir yok")
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        curr = result.at[i, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim %"] = round((curr - prev) / prev * 100, 2)

    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result


def sheets_yaz(sheet, sid, sayfa, veri):
    sheet.values().update(
        spreadsheetId=sid, range=f"{sayfa}!A1",
        valueInputOption="RAW", body={"values": veri}
    ).execute()


def sayfa_ekle(sheet, sid, baslik):
    sheet.batchUpdate(
        spreadsheetId=sid,
        body={"requests": [{"addSheet": {"properties": {"title": baslik}}}]}
    ).execute()


def google_sheets_guncelle(df_tumu):
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON yok!")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets", "v4", credentials=creds).spreadsheets()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # Tum Veri
    if "Tum Veri" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ilk_id, "title": "Tum Veri"},
                "fields": "title"}}
        ]}).execute()
        mevcut[0] = "Tum Veri"

    cols = list(df_tumu.columns)
    rows_all = [[str(v) for v in r] for r in df_tumu.values.tolist()]
    sheets_yaz(svc, SPREADSHEET_ID, "Tum Veri",
               [[f"Guncelleme: {now}"] + [""]*(len(cols)-1)] + [cols] + rows_all)
    print(f"  Tum Veri: {len(rows_all)} satir OK")

    for hisse in HISSELER:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        if df_h.empty:
            continue
        if hisse not in mevcut:
            sayfa_ekle(svc, SPREADSHEET_ID, hisse)
        sheets_yaz(svc, SPREADSHEET_ID, hisse,
                   [cols] + [[str(v) for v in r] for r in df_h.values.tolist()])
        print(f"  {hisse}: OK")

    if "Ozet" not in mevcut:
        sayfa_ekle(svc, SPREADSHEET_ID, "Ozet")
    ozet = [["Hisse","Son Kapanis","Degisim %","30G Yuksek","30G Dusuk","Guncelleme"]]
    for hisse in HISSELER:
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        if df_h.empty:
            continue
        son = df_h.iloc[-1]
        ozet.append([hisse, str(son["Kapanis"]), str(son["Degisim %"]),
                     str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), now])
    sheets_yaz(svc, SPREADSHEET_ID, "Ozet", ozet)
    print("  Ozet: OK")


def main():
    print("="*40)
    print(f"BIST - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("="*40)
    print(f"Spreadsheet ID: {SPREADSHEET_ID[:20]}...")

    tum_df = []
    for hisse in HISSELER:
        print(f"\n{hisse}...")
        df = veri_cek(hisse, GECMIS_GUN)
        if not df.empty:
            tum_df.append(df)
        time.sleep(1)

    if not tum_df:
        raise SystemExit("HATA: Hic veri yok!")

    df_tumu = pd.concat(tum_df, ignore_index=True).sort_values(["Hisse","Tarih"])
    print(f"\nToplam: {len(df_tumu)} satir")
    google_sheets_guncelle(df_tumu)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
