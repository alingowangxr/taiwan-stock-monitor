# -*- coding: utf-8 -*-
import os, time, random, requests, json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數設定 ==========
MARKET_CODE = "us-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "us_stock_list_cache.json")

# 效能優化：5 個執行緒
MAX_WORKERS = 5 
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 💡 定義數據過期時間 (例如 3600 秒 = 1 小時)
# 這樣盤中執行時，若檔案超過一小時就會強制更新
DATA_EXPIRY_SECONDS = 3600 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def classify_security(name: str, is_etf: bool) -> str:
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """獲取美股清單，具備今日快取門檻"""
    threshold = 3000
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日美股快取 ({len(data)} 檔)...")
                        return data
        except: pass

    log("📡 開始從官網獲取美股普通股清單...")
    all_rows = []
    for site in ["nasdaqlisted.txt", "otherlisted.txt"]:
        try:
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/{site}"
            r = requests.get(url, timeout=15)
            df = pd.read_csv(StringIO(r.text), sep="|")
            df = df[df["Test Issue"] == "N"]
            sym_col = "Symbol" if site == "nasdaqlisted.txt" else "NASDAQ Symbol"
            df["Category"] = df.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
            valid_df = df[df["Category"] == "Common Stock"]
            for _, row in valid_df.iterrows():
                ticker = str(row[sym_col]).strip().replace('$', '-')
                name = str(row['Security Name']).strip()
                all_rows.append(f"{ticker}&{name}")
            time.sleep(1) 
        except Exception as e:
            log(f"⚠️ {site} 獲取失敗: {e}")

    final_list = list(set(all_rows))
    if len(final_list) >= threshold:
        with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False)
        log(f"✅ 美股清單更新完成，共 {len(final_list)} 檔。")
        return final_list
    return final_list

def download_stock_data(item):
    """單檔下載：具備時間檢查機制以利盤中更新"""
    yf_tkr = "Unknown"
    try:
        yf_tkr, name = item.split('&', 1)
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '_', '-')]).strip()
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        # 💡 關鍵修改：檢查檔案是否「存在」且「夠新」
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            # 如果檔案小於 1 小時，且檔案大小正常，則視為有效數據，不重複下載
            if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "tkr": yf_tkr}

        # 若檔案過期或不存在，執行下載
        time.sleep(random.uniform(0.4, 1.2))
        tk = yf.Ticker(yf_tkr)
        
        for attempt in range(2):
            try:
                hist = tk.history(period="2y", timeout=20)
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    if 'date' in hist.columns:
                        hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None)
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "tkr": yf_tkr}
            except Exception as e:
                if "Rate limited" in str(e):
                    time.sleep(random.uniform(20, 40))
            time.sleep(random.uniform(2, 4))
        return {"status": "empty", "tkr": yf_tkr}
    except:
        return {"status": "error"}

def main():
    start_time = time.time()
    items = get_full_stock_list()
    if not items: 
        log("❌ 無法取得清單。")
        return {"total": 0, "success": 0, "fail": 0}

    log(f"🚀 開始美股下載任務 (共 {len(items)} 檔)")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="美股下載進度")
        for future in as_completed(futures):
            res = future.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()
    
    total_expected = len(items)
    effective_success = stats['success'] + stats['exists']
    fail_count = stats['error'] + stats['empty']

    download_stats = {
        "total": total_expected,
        "success": effective_success,
        "fail": fail_count
    }

    duration = (time.time() - start_time) / 60
    log("="*30)
    log(f"🏁 美股下載完成 (耗時 {duration:.1f} 分鐘)")
    log(f"   - 下載成功(含舊檔): {effective_success}")
    log(f"   - 數據完整度: {(effective_success/total_expected)*100:.2f}%")
    log("="*30)
    
    return download_stats

if __name__ == "__main__":
    main()
