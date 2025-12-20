# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
from datetime import datetime

import downloader_tw
import analyzer
import notifier

def run_market_pipeline(market_id, market_name, emoji):
    print("\n" + "="*50)
    print(f"{emoji} 開始處理 {market_name} ({market_id})")
    print("="*50)

    print(f"【Step 1: 下載/更新 {market_name} 數據】")
    try:
        if market_id == "tw-share":
            downloader_tw.main()
    except Exception as e:
        print(f"❌ 下載過程出錯: {e}")

    print(f"\n【Step 2: 執行 {market_name} 數據分析 & 繪圖】")
    try:
        # 取得分析結果：圖片清單、數據表、文字報表
        img_paths, report_df, text_reports = analyzer.run_global_analysis(market_id=market_id)
        
        if report_df.empty:
            print(f"⚠️ {market_name} 分析結果為空，跳過寄信。")
            return

        print(f"\n【Step 3: 寄送 {market_name} 專業報表】")
        # ✅ 修正點：對接 notifier.py 的正確函式名稱與參數
        notifier.send_stock_report(
            market_name=market_name,
            img_data=img_paths,
            report_df=report_df,
            text_reports=text_reports
        )
        # 註：因為 notifier.py 內部已有錯誤處理並會列印結果，這裡簡化呼叫即可

    except Exception as e:
        print(f"❌ 分析或寄信過程出錯: {e}")

def main():
    start_time = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"🚀 全球股市監控系統啟動 | 開始時間: {now_str}")

    # 只保留台灣股市
    markets = [
        {"id": "tw-share", "name": "台灣股市", "emoji": "🇹🇼"}
    ]

    for m in markets:
        run_market_pipeline(m["id"], m["name"], m["emoji"])

    end_time = time.time()
    total_duration = (end_time - start_time) / 60
    print("\n" + "="*50)
    print(f"🎉 所有市場處理完畢！總耗時: {total_duration:.2f} 分鐘")
    print("="*50)

if __name__ == "__main__":
    main()
