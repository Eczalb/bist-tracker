import os
import json
import pandas as pd
import urllib.request
import urllib.parse
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


def veri_cek(hisse, gun=30):
    """Yahoo Finance'den CSV olarak veri ceker."""
    sembol = f"{hisse}.IS"
    bitis = int(datetime.today().timestamp())
    baslangic = int((datetime.today() - timedelta(days=gun + 15)).timestamp())

    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/{urllib.parse.quote(sembol)}"
        f"?period1={baslangic}&period2={bitis}&interval=1d&events=history"
    )

    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8")
    except Exception as e:
        print(f"  HATA {hisse}: {e}")
        return pd.DataFrame()

    lines = [l for l in data.strip().split("\n") if l and "null" not in l]
    if len(lines) < 2:
        print(f"  {hisse} icin veri yok.")
        return pd.DataFrame()

    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        try:
            rows.append({
                "Hisse": hisse,
                "Tarih": parts[0],
                "Acilis": round(float(parts[1]), 2),
                "Yuksek": round(float(parts[2]), 2),
                "Dusuk": round(float(parts[3]), 2),
                "Kapanis": round(float(parts[4]), 2),
                "Hacim": int(float(parts[6]) if len(parts) > 6 else parts[5]),
            })
        except Exception:
            continue

    if not rows:
        print(f"  {hisse} satirlar islenmedi.")
        return pd.DataFrame()

    df = pd.DataFrame(rows).tail(gun)
    prev = df["Kapanis"].shift(1)
    df["Degisim %"] = ((df["Kapanis"] - prev) / prev * 100).round(2).fillna(0)
    print(f"  OK {hisse}: {len(df)} satir, son kapanis {df.iloc[-1]['Kapanis']} TL")
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

    guncelleme = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Tum Veri sayfasi
    baslık = [df_tumu.columns.tolist()]
    satirlar = [[str(v) for v in row] for row in df_tumu.values.tolist()]
    bilgi = [[f"Son guncelleme: {guncelleme} UTC", "", "", "", "", "", "", ""]]
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID, range="Tum Veri!A1",
        valueInputOption="RAW", body={"values": bilgi + baslık + satirlar}
    ).execute()
    print(f"  Tum Veri guncellendi ({len(satirlar)} satir)")

    # Mevcut sayfalari al
    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    mevcut = [s["properties"]["title"] for s in meta.get("sheets", [])]

    # Eksik sayfalari olustur
    istekler = []
    for h in list(df_tumu["Hisse"].unique()) + ["Ozet"]:
        if h not in mevcut:
            istekler.append({"addSheet": {"properties": {"title": h}}})
    if istekler:
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID,
                          body={"requests": istekler}).execute()

    # Her hisse icin ayri sayfa
    for hisse in df_tumu["Hisse"].unique():
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        veri = [df_h.columns.tolist()] + [[str(v) for v in r] for r in df_h.values.tolist()]
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID, range=f"{hisse}!A1",
            valueInputOption="RAW", body={"values": veri}
        ).execute()

    # Ozet sayfasi
    ozet = [["Hisse", "Son Kapanis", "Degisim %", "30G En Yuksek", "30G En Dusuk", "Guncelleme"]]
    for hisse in df_tumu["Hisse"].unique():
        df_h = df_tumu[df_tumu["Hisse"] == hisse]
        son = df_h.iloc[-1]
        ozet.append([hisse, str(son["Kapanis"]), str(son["Degisim %"]),
                     str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), guncelleme])
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID, range="Ozet!A1",
        valueInputOption="RAW", body={"values": ozet}
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

    if not tum_df:
        print("Hic veri cekilemedi!")
        return

    df_tumu = pd.concat(tum_df, ignore_index=True).sort_values(["Hisse", "Tarih"])
    print(f"\nToplam {len(df_tumu)} satir Google Sheets'e yaziliyor...")
    google_sheets_guncelle(df_tumu)
    print("\nTamamlandi!")


if __name__ == "__main__":
    main()
