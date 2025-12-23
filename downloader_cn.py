# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "cn_stock_list_cache.json")

# 🛡️ 穩定性優先：保持 4 個執行緒，這是對 GitHub Actions 最穩定的設定
THREADS_CN = 4 
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def get_cn_list():
    """獲取 A 股清單：整合 EM 接口與多重保底機制"""
    ensure_pkg("akshare")
    import akshare as ak
    threshold = 4500  
    
    # 1. 檢查今日快取
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日快取 (共 {len(data)} 檔)")
                        return data
        except Exception as e:
            log(f"⚠️ 快取讀取失敗: {e}")

    # 2. 獲取清單
    log("📡 嘗試從 Akshare EM 接口獲取清單...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        res = [f"{row['code']}&{row['名稱']}" if '名稱' in row else f"{row['code']}&{row['名称']}" for _, row in df.iterrows()]
        
        if len(res) >= threshold:
            with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False)
            log(f"✅ 成功獲取 {len(res)} 檔標的")
            return res
    except Exception as e:
        log(f"⚠️ EM 接口失敗: {e}")

    # 3. 歷史備援
    if os.path.exists(CACHE_LIST_PATH):
        log("🔄 接口全數失敗，使用歷史快取備援...")
        with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    return ["600519&貴州茅台", "000001&平安銀行", "300750&寧德時代"]

def download_one(item):
    """強化穩定版下載邏輯：針對 A 股風控優化"""
    code, name = item.split('&', 1)
    symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
    out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

    # 續跑機制
    if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
        return {"status": "exists", "code": code}

    #
