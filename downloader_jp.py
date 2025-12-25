# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝/匯入必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

MANIFEST_CSV = Path(LIST_DIR) / "jp_manifest.csv"
THREADS = 4

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_tse_list():
    """利用 tokyo-stock-exchange 套件獲取日股清單"""
    log("📡 正在透過套件獲取日股清單...")
    try:
        # 讀取套件內建的 CSV 檔案路徑
        df = pd.read_csv(tse.csv_file_path)
        # 提取代號 (Code) 與 名稱 (Name)
        # 日股代號在 yfinance 需補成 4 位加 .T
        res = []
        for _, row in df.iterrows():
            code = str(row['Code']).strip()
            if len(code) == 4 and code.isdigit():
                res.append({"code": code, "name": row['Name'], "board": "T"})
        return pd.DataFrame(res)
    except Exception as e:
        log(f"⚠️ 套件讀取失敗: {e}")
        return pd.DataFrame([{"code":"7203","name":"TOYOTA","board":"T"}])

def build_manifest(df_list):
    if MANIFEST_CSV.exists():
        return pd.read_csv(MANIFEST_CSV)
    df_list["status"] = "pending"
    existing_files = {f.split(".")[0] for f in os.listdir(DATA_DIR) if f.endswith(".T.csv")}
    df_list.loc[df_list['code'].astype(str).isin(existing_files), "status"] = "done"
    df_list.to_csv(MANIFEST_CSV, index=False)
    return df_list

def download_one(row_tuple):
    idx, row = row_tuple
    code = str(row['code']).zfill(4)
    symbol = f"{code}.T"
    out_path = os.path.join(DATA_DIR, f"{code}.T.csv")
    
    try:
        tk = yf.Ticker(symbol)
        df_raw = tk.history(period="2y", interval="1d", auto_adjust=False)
        if df_raw is not None and not df_raw.empty:
            df_raw.reset_index(inplace=True)
            df_raw.columns = [c.lower() for c in df_raw.columns]
            df_raw['date'] = pd.to_datetime(df_raw['date'], utc=True).dt.tz_localize(None)
            df_raw = df_raw[['date','open','high','low','close','volume']]
            df_raw.to_csv(out_path, index=False)
            return idx, "done"
        return idx, "empty"
    except:
        return idx, "failed"

def main():
    log("🇯🇵 啟動日本股市下載引擎 (TSE)")
    df_list = get_tse_list()
    mf = build_manifest(df_list)
    todo = mf[mf["status"] != "done"]

    if todo.empty:
        log("✅ 所有日股資料已是最新。")
        return

    log(f"📝 待處理標的：{len(todo)} 檔")
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
        pbar = tqdm(total=len(todo), desc="日股進度")
        for f in as_completed(futures):
            idx, status = f.result()
            mf.at[idx, "status"] = status
            pbar.update(1)
            if idx % 50 == 0: mf.to_csv(MANIFEST_CSV, index=False)
        pbar.close()
    mf.to_csv(MANIFEST_CSV, index=False)
    log(f"🏁 日股任務完成！")

if __name__ == "__main__":
    main()
