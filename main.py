import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import time
import sys
import io
import os
import json
import datetime

# --- 設定エリア ---
# 同時接続数（5〜10推奨）
CONCURRENT_REQUESTS = 10 
# 1回に処理する件数（これごとに休憩する）
BATCH_SIZE = 500
# バッチ間の休憩時間（秒）
BATCH_INTERVAL = 2
# ロックファイルの名前（これを「使用中」の札として使う）
LOCK_FILE = "system_lock.json"

# Windows用設定
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

# --- 1. ロック制御（交通整理）システム ---
def get_lock_status():
    """現在誰かが使っているかチェックする"""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return True, data
        except:
            return False, None
    return False, None

def acquire_lock(user_name, total_items):
    """使用中札を掲げる"""
    if os.path.exists(LOCK_FILE):
        return False # 既に誰かが使っている
    
    start_time = datetime.datetime.now().strftime('%H:%M:%S')
    data = {
        "user": user_name,
        "start_time": start_time,
        "total": total_items,
        "status": "Running"
    }
    with open(LOCK_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    return True

def release_lock():
    """使用中札を下ろす"""
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

# --- 2. 通信ロジック ---
async def fetch_single(session, url):
    try:
        async with session.get(url, timeout=20, ssl=False, headers=HEADERS) as response:
            await response.read()
            return {"status": "OK", "code": response.status, "msg": "OK"}
    except asyncio.TimeoutError:
        return {"status": "Error", "code": "Timeout", "msg": "タイムアウト"}
    except aiohttp.ClientConnectorError:
        return {"status": "Error", "code": "ConnectError", "msg": "接続不可"}
    except Exception as e:
        return {"status": "Error", "code": "Error", "msg": str(e)}

async def fetch_url_with_retry(session, url, row_index):
    res = await fetch_single(session, url)
    if res["status"] == "OK":
        return row_index, res["code"], res["msg"]
    
    retry_url = None
    if url.startswith("http:"):
        retry_url = url.replace("http:", "https:", 1)
    elif url.startswith("https:"):
        retry_url = url.replace("https:", "http:", 1)
    
    if retry_url:
        res_retry = await fetch_single(session, retry_url)
        if res_retry["status"] == "OK":
            return row_index, res_retry["code"], f"OK (自動切替: {retry_url})"
    
    return row_index, res["code"], res["msg"]

async def process_batch(urls, start_index):
    """指定されたリストの一部だけを処理する"""
    results = [None] * len(urls)
    messages = [None] * len(urls)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, ssl=False)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i, url in enumerate(urls):
            real_index = start_index + i
            url_str = str(url).strip()
            if url_str.startswith("http"):
                task = fetch_url_with_retry(session, url_str, real_index)
                tasks.append(task)
            else:
                results[i] = "URL不正"
                messages[i] = "http/httpsなし"

        for task in asyncio.as_completed(tasks):
            # ※ここで返ってくるindexは全体通し番号
            row_index, status_code, msg = await task
            # バッチ内の相対位置に戻す
            local_index = row_index - start_index
            results[local_index] = status_code
            messages[local_index] = msg
            
    return results, messages

# --- 3. スタイル設定 ---
def highlight_bad_rows(row):
    val = str(row['Status_Code'])
    bad_list = ['ConnectError', 'Timeout', 'URL不正', '404', 'Error']
    if val in bad_list:
        return ['background-color: #ffcccc; color: #990000; font-weight: bold'] * len(row)
    return [''] * len(row)

# --- 4. メイン画面 ---
st.set_page_config(page_title="チーム用Webチェッカー", layout="wide")

# 現在の状況を表示
is_locked, lock_info = get_lock_status()

st.title("🚦 チーム用 Webサイト生存チェッカー")

if is_locked:
    st.error(f"⛔ 現在、他のメンバーが使用中です！")
    st.info(f"👤 利用者: **{lock_info['user']}** さん")
    st.info(f"🕒 開始時間: {lock_info['start_time']} / 📦 件数: {lock_info['total']} 件")
    st.warning("処理が終わるまでお待ちください。画面をリロードすると状況が更新されます。")
    
    # 万が一の強制解除ボタン（誰かがブラウザ閉じちゃった時用）
    with st.expander("⚠️ 前の人が終わってるのにずっとロックされている場合"):
        if st.button("強制的にロックを解除する"):
            release_lock()
            st.rerun()
            
    # ロック中は以下を表示しない
    st.stop() 

else:
    st.success("✅ 現在空いています。利用可能です。")

# --- 入力エリア ---
col1, col2 = st.columns([1, 2])
with col1:
    user_name = st.text_input("あなたの名前", placeholder="例: 山田")
with col2:
    uploaded_file = st.file_uploader("リストをアップロード (数千件でもOK)", type=['xlsx', 'csv'])

if uploaded_file is not None and user_name:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        st.write(f"📂 読み込み完了: {len(df)} 件")
        columns = df.columns.tolist()
        url_col = st.selectbox("URL列を選択", columns)
        
        if st.button("🚀 チェック開始"):
            # ロック獲得を試みる
            if not acquire_lock(user_name, len(df)):
                st.error("タッチの差で他の人が開始しました！リロードしてください。")
                st.stop()
            
            try:
                # 全データ数
                total_rows = len(df)
                all_statuses = [None] * total_rows
                all_msgs = [None] * total_rows
                urls = df[url_col].tolist()
                
                start_time = time.time()
                
                # --- 自動分割ループ ---
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # BATCH_SIZE ずつ切り出して処理
                for i in range(0, total_rows, BATCH_SIZE):
                    batch_urls = urls[i : i + BATCH_SIZE]
                    status_text.text(f"処理中... {i} ～ {min(i + BATCH_SIZE, total_rows)} 件目 / 全 {total_rows} 件")
                    
                    # Async処理実行
                    # Windows対応のループ取得
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    batch_statuses, batch_msgs = loop.run_until_complete(process_batch(batch_urls, i))
                    loop.close()
                    
                    # 結果を結合
                    for j, res in enumerate(batch_statuses):
                        all_statuses[i + j] = res
                        all_msgs[i + j] = batch_msgs[j]
                    
                    # 進捗更新
                    progress_bar.progress(min((i + BATCH_SIZE) / total_rows, 1.0))
                    
                    # 休憩（サーバーへの配慮）
                    if i + BATCH_SIZE < total_rows:
                        time.sleep(BATCH_INTERVAL)
                
                # --- 完了処理 ---
                df['Status_Code'] = all_statuses
                df['Message'] = all_msgs
                
                end_time = time.time()
                minutes = (end_time - start_time) / 60
                
                st.success(f"✅ すべて完了しました！ ({minutes:.1f}分)")
                
                # 色付きExcel出力
                styler = df.style.apply(highlight_bad_rows, axis=1)
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    styler.to_excel(writer, index=False, sheet_name='Result')
                
                st.download_button(
                    label="📥 結果をダウンロード",
                    data=buffer.getvalue(),
                    file_name=f'check_result_{user_name}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                )
                
            except Exception as e:
                st.error(f"エラー: {e}")
            finally:
                # 終わったら（またはエラーでも）必ずロック解除
                release_lock()
                st.info("ロックを解除しました。")

    except Exception as e:
        st.error(f"ファイル読み込みエラー: {e}")
elif uploaded_file is not None and not user_name:
    st.warning("⚠️ 名前を入力してください（他のメンバーに通知するためです）")