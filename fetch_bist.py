import os, json, time
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

    # MultiIndex durumu — yfinance yeni surumde (ticker, Price) seklinde geliyor
    if isinstance(ham.columns, pd.MultiIndex):
        # Level 0: Price tipi (Close, High...), Level 1: Ticker (THYAO.IS)
        # VEYA Level 0: Ticker, Level 1: Price tipi
        # Hangi seviyede ticker oldugunu bul
        level0_vals = ham.columns.get_level_values(0).tolist()
        level1_vals = ham.columns.get_level_values(1).tolist()
        if sembol in level0_vals:
            # Level 0 = ticker -> level 1 = price type
            ham = ham.xs(sembol, axis=1, level=0)
        elif sembol in level1_vals:
            # Level 1 = ticker -> level 0 = price type
            ham = ham.xs(sembol, axis=1, level=1)
        else:
            # Son care: hangi level'da price tipi var
            if "Close" in level0_vals:
                ham.columns = ham.columns.droplevel(1)
            else:
                ham.columns = ham.columns.droplevel(0)

    print(f"  {hisse} sutunlar: {list(ham.columns)}")

    gerekli = ["Open", "High", "Low", "Close", "Volume"]
    eksik = [s for s in gerekli if s not in ham.columns]
    if eksik:
        print(f"  {hisse}: EKSIK {eksik}")
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
                "Degisim": 0.0,
            })
        except Exception as ex:
            continue

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    for i in range(1, len(result)):
        prev = result.at[i-1, "Kapanis"]
        if prev and prev != 0:
            result.at[i, "Degisim"] = round((result.at[i, "Kapanis"] - prev) / prev * 100, 2)

    print(f"  OK {hisse}: {len(result)} gun, son: {result.iloc[-1]['Kapanis']} TL")
    return result

def yaz(svc, sid, sayfa, veri):
    svc.values().update(spreadsheetId=sid, range=f"'{sayfa}'!A1",
        valueInputOption="RAW", body={"values": veri}).execute()

def sayfa_ekle(svc, sid, baslik):
    svc.batchUpdate(spreadsheetId=sid,
        body={"requests": [{"addSheet": {"properties": {"title": str(baslik)}}}]}).execute()

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
    print(f"  Mevcut sayfalar: {mevcut}")

    # Tum Veri
    if "Tum Veri" not in mevcut:
        ilk_id = meta["sheets"][0]["properties"]["sheetId"]
        svc.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [
            {"updateSheetProperties": {"properties": {"sheetId": ilk_id, "title": "Tum Veri"}, "fields": "title"}}
        ]}).execute()
        mevcut[0] = "Tum Veri"

    cols = list(df.columns)
    rows_all = [[str(v) for v in r] for r in df.values.tolist()]
    yaz(svc, SPREADSHEET_ID, "Tum Veri",
        [[f"Guncelleme: {now}"] + [""]*(len(cols)-1), cols] + rows_all)
    print(f"  Tum Veri: {len(rows_all)} satir OK")

    for h in HISSELER:
        df_h = df[df["Hisse"] == h]
        if df_h.empty:
            print(f"  {h}: veri yok, atlandi")
            continue
        if h not in mevcut:
            sayfa_ekle(svc, SPREADSHEET_ID, h)
        yaz(svc, SPREADSHEET_ID, h, [cols] + [[str(v) for v in r] for r in df_h.values.tolist()])
        print(f"  {h}: {len(df_h)} satir OK")

    if "Ozet" not in mevcut:
        sayfa_ekle(svc, SPREADSHEET_ID, "Ozet")
    ozet = [["Hisse","Son Kapanis","Degisim %","30G Yuksek","30G Dusuk","Guncelleme"]]
    for h in HISSELER:
        df_h = df[df["Hisse"] == h]
        if df_h.empty: continue
        son = df_h.iloc[-1]
        ozet.append([h, str(son["Kapanis"]), str(son["Degisim"]),
                     str(df_h["Yuksek"].max()), str(df_h["Dusuk"].min()), now])
    yaz(svc, SPREADSHEET_ID, "Ozet", ozet)
    print("  Ozet: OK")

def main():
    print("="*40)
    print(f"BIST - {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"ID: {SPREADSHEET_ID[:25]}...")
    print("="*40)
    tum = []
    for h in HISSELER:
        print(f"\n{h}...")
        df = veri_cek(h, GECMIS_GUN)
        if not df.empty:
            tum.append(df)
        time.sleep(1)
    if not tum:
        raise SystemExit("HATA: Hic veri yok!")
    df_tum = pd.concat(tum, ignore_index=True).sort_values(["Hisse","Tarih"])
    print(f"\nToplam: {len(df_tum)} satir")
    guncelle(df_tum)
    print("\nTamamlandi!")

if __name__ == "__main__":
    main()
