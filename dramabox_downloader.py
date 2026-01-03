import os
import time
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURASI ---
HISTORY_FILE = "download_history.xlsx"
BASE_DOWNLOAD_API = "https://dramabox-api-rho.vercel.app/download"
BASE_HOME_API = "https://dramabox-api-rho.vercel.app/api/home"
MAX_WORKERS = 5 # Jumlah thread download bersamaan (Parallel)

# --- UTILITIES ---

def format_size(size_bytes):
    """Mengubah ukuran bytes menjadi format yang mudah dibaca (MB, GB)."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(0)
    p = 1024
    s = size_bytes
    while s >= p and i < len(size_name) - 1:
        s /= p
        i += 1
    return f"{s:.2f} {size_name[i]}"

def get_with_retry(url, retries=3, delay=1, timeout=30):
    """Helper function untuk request dengan retry logic."""
    for i in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            return response
        except requests.exceptions.RequestException:
            time.sleep(delay * (i + 1)) 
    raise Exception("Max retries exceeded")

def get_head_with_retry(url, retries=3, timeout=10):
    """Helper untuk HEAD request yang lebih robust."""
    for i in range(retries):
        try:
            return requests.head(url, allow_redirects=True, timeout=timeout)
        except:
             time.sleep(1)
    return None

def get_file_size(url):
    """Mendapatkan ukuran file dari header HTTP."""
    try:
        response = get_head_with_retry(url, timeout=10)
        if response is None or response.status_code != 200:
             # Fallback ke GET
             try:
                response = requests.get(url, stream=True, allow_redirects=True, timeout=15)
                val = int(response.headers.get('content-length', 0))
                response.close()
                return val
             except:
                return 0
        
        if response:
            return int(response.headers.get('content-length', 0))
    except:
        return 0
    return 0

def download_video_file_chunked(url, filename, folder, pbar_pos=0):
    """
    Download video dengan support multi-threaded progress bar.
    """
    path = os.path.join(folder, filename)
    
    # Resume check sederhana (skip jika ukuran file sudah sama)
    remote_size = get_file_size(url)
    if os.path.exists(path):
        local_size = os.path.getsize(path)
        if remote_size > 0 and local_size == remote_size:
            return True, f"{filename} (Skipped)"

    try:
        response = requests.get(url, stream=True, timeout=60)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192 # Ukuran chunk lebih besar untuk speed
        
        # Pbar per file (hanya jika mode single, kalau multi nanti berantakan jadi kita silent saja atau pakai leave=False)
        # Untuk parallel download, tqdm agak tricky. Kita return status saja.
        
        with open(path, 'wb') as file:
            for data in response.iter_content(block_size):
                file.write(data)
                
        return True, filename
    except Exception as e:
        return False, f"{filename} Error: {e}"

# --- EXCEL HISTORY MANAGER ---

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            return pd.read_excel(HISTORY_FILE)
        except Exception:
            return pd.DataFrame(columns=["BookID", "Title", "TotalChapters", "DownloadDate", "Status"])
    return pd.DataFrame(columns=["BookID", "Title", "TotalChapters", "DownloadDate", "Status"])

def save_to_history(book_id, title, total_chapters, status="Completed"):
    df = load_history()
    new_data = {
        "BookID": str(book_id),
        "Title": title,
        "TotalChapters": total_chapters,
        "DownloadDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Status": status
    }
    df['BookID'] = df['BookID'].astype(str)
    df = df[df["BookID"] != str(book_id)]
    df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
    try:
        df.to_excel(HISTORY_FILE, index=False)
    except: pass

def check_is_downloaded(book_id):
    df = load_history()
    if df.empty: return None
    df['BookID'] = df['BookID'].astype(str)
    result = df[df["BookID"] == str(book_id)]
    if not result.empty: return result.iloc[0]
    return None

# --- CORE LOGIC ---

def process_book_parallel(book_data):
    """
    Download satu buku secara PARALLEL (Multi-threading).
    Jauh lebih cepat daripada download satu-satu.
    """
    book_id = str(book_data['id'])
    book_name = book_data['name']
    
    # 1. Fetch Detail
    try:
        response = get_with_retry(f"{BASE_DOWNLOAD_API}/{book_id}", timeout=20)
        data_json = response.json()
    except Exception as e:
        print(f"   [Error] Gagal ambil detail {book_name}: {e}")
        return False

    if data_json.get("status") != "success":
        return False

    chapters = data_json.get("data", [])
    total_chapters = len(chapters)
    if total_chapters == 0: return False

    # 2. Folder
    safe_name = "".join([c for c in book_name if c.isalnum() or c in (' ', '_', '-')]).strip()
    folder_name = f"downloads_{book_id}_{safe_name[:30]}"
    if not os.path.exists(folder_name): os.makedirs(folder_name)
    
    print(f"\n   >> Mendownload: {book_name} ({total_chapters} Eps) | Mode: Parallel ({MAX_WORKERS} threads)")

    # 3. Parallel Download
    success_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {}
        
        # Submit semua chapters ke thread pool
        for chapter in chapters:
            c_id = chapter.get("chapterId")
            c_idx = chapter.get("chapterIndex")
            url = chapter.get("videoPath")
            filename = f"{c_id}_{c_idx}.mp4"
            
            future = executor.submit(download_video_file_chunked, url, filename, folder_name)
            future_to_file[future] = filename

        # Proses hasil saat selesai (as_completed)
        # Gunakan tqdm di sini sebagai progress bar global untuk buku ini
        with tqdm(total=total_chapters, unit="file", desc="      Progress") as pbar:
            for future in as_completed(future_to_file):
                status, msg = future.result()
                if status:
                    success_count += 1
                pbar.update(1)

    # 4. Save History
    status = "Partial" if success_count < total_chapters else "Completed"
    if success_count > 0:
        save_to_history(book_id, book_name, total_chapters, status)
        
    print(f"      Selesai. {success_count}/{total_chapters} berhasil.")
    return True

def fetch_all_available_books():
    """Scan semua buku (dengan proteksi duplikat)."""
    all_books = []
    seen_ids = set() 
    page = 1
    max_dup = 0
    
    print("\nSedang memindai seluruh katalog server...")
    
    while True:
        url = f"{BASE_HOME_API}?page={page}&size=10"
        try:
            resp = get_with_retry(url, timeout=15, delay=0.5).json()
            if resp.get("success"):
                data = resp.get("data", {})
                books = data.get("book", [])
                
                if not books: break
                
                new_in_page = 0
                for b in books:
                    bid = str(b['id'])
                    if bid not in seen_ids:
                        seen_ids.add(bid)
                        all_books.append(b)
                        new_in_page += 1
                
                print(f"   Page {page}: Ditemukan {len(books)} buku. ({new_in_page} baru)", end="\r")
                
                if new_in_page == 0:
                    max_dup += 1
                    if max_dup >= 2: 
                        print(f"\n   [Stop] Loop terdeteksi. Stop di page {page}.")
                        break
                else: max_dup = 0
                
                if not data.get("isMore") or page > 500: break
                page += 1
            else: break
        except: break
            
    print(f"\n   Selesai scan! Total unik: {len(all_books)} buku.")
    return all_books

def menu_download_auto_all():
    # 1. Fetch
    all_books = fetch_all_available_books()
    if not all_books: return

    # 2. Filter
    queue = []
    print("\nMemfilter history...")
    for b in all_books:
        bid = str(b['id'])
        hist = check_is_downloaded(bid)
        if hist is None or int(b.get('chapterCount', 0)) > int(hist.get("TotalChapters", 0)):
            queue.append(b)

    if not queue:
        print("Semua sudah up-to-date!")
        return

    print(f"\nDitemukan {len(queue)} buku yang perlu didownload.")

    # 3. Hitung Size (Opsional tapi diminta user)
    print("\nMenghitung total ukuran file (Ctrl+C untuk Skip)...")
    total_size = 0
    
    try:
        # Kita pakai Parallel juga biar hitung sizenya CEPAT!
        def get_book_size(book):
            b_size = 0
            try:
                bid = str(book['id'])
                r = requests.get(f"{BASE_DOWNLOAD_API}/{bid}", timeout=15).json()
                if r['status'] == 'success':
                    for c in r['data']:
                        if c.get('videoPath'):
                            b_size += get_file_size(c.get('videoPath'))
            except: pass
            return b_size

        with ThreadPoolExecutor(max_workers=10) as executor: # 10 threads untuk cek size
            futures = {executor.submit(get_book_size, b): b for b in queue}
            for future in tqdm(as_completed(futures), total=len(queue), desc="Checking Size"):
                total_size += future.result()
                
    except KeyboardInterrupt:
        print("\n   [Info] Skip cek size.")
    
    print(f"\nTOTAL QUEUE: {len(queue)} Buku.")
    print(f"ESTIMATED SIZE: {format_size(total_size)}")
    
    if input("Lanjut Download? (y/n): ").lower() != 'y': return

    # 4. Execute
    start = time.time()
    for i, book in enumerate(queue):
        print(f"\n--- [{i+1}/{len(queue)}] {book['name']} ---")
        process_book_parallel(book)
    
    print(f"\nSelesai dalam {(time.time()-start)/60:.2f} menit.")

def download_specific_chapter():
    """Download hanya SATU episode spesifik berdasarkan Book ID dan Chapter Index."""
    print("\n--- DOWNLOAD SPESIFIK EPISODE ---")
    book_id = input("Masukkan Book ID: ").strip()
    if not book_id: return

    try:
        idx_input = input("Masukkan Chapter Index (mulai dari 0, atau 1, 2, ...): ").strip()
        target_index = int(idx_input)
    except ValueError:
        print("Index harus angka.")
        return

    # 1. Fetch Data
    print("Mengambil data buku...")
    try:
        response = get_with_retry(f"{BASE_DOWNLOAD_API}/{book_id}", timeout=20)
        data = response.json()
    except Exception as e:
        print(f"Error API: {e}")
        return

    if data.get("status") != "success":
        print("Buku tidak ditemukan atau API error.")
        return

    chapters = data.get("data", [])
    if not chapters:
        print("Tidak ada chapter di buku ini.")
        return
        
    # 2. Cari Chapter yang dimau
    target_chapter = None
    for chap in chapters:
        if int(chap.get("chapterIndex", -1)) == target_index:
            target_chapter = chap
            break
    
    if not target_chapter:
        print(f"Chapter dengan index {target_index} tidak ditemukan.")
        return

    # 3. Download
    c_id = target_chapter.get("chapterId")
    c_idx = target_chapter.get("chapterIndex")
    url = target_chapter.get("videoPath")
    filename = f"{c_id}_{c_idx}.mp4"
    
    # Buat folder
    folder_name = f"downloads_{book_id}_manual"
    if not os.path.exists(folder_name): os.makedirs(folder_name)
    
    print(f"\nMendownload: {filename} ke folder '{folder_name}'...")
    
    success, msg = download_video_file_chunked(url, filename, folder_name)
    
    if success:
        print(f"BERHASIL! File tersimpan: {os.path.join(folder_name, filename)}")
    else:
        print(f"GAGAL: {msg}")

def main():
    while True:
        print("\n=== DRAMABOX TURBO DOWNLOADER (MULTI-THREAD) ===")
        print("1. Manual ID (Full Book)")
        print("2. Auto Scan & Download All")
        print("3. Cek History Excel")
        print("4. Repair/Download Spesifik Episode")
        print("5. Exit")
        c = input("Pilih: ")
        
        if c=='1': 
            bid = input("ID: ")
            if bid: process_book_parallel({'id': bid, 'name': 'Manual'})
        elif c=='2': menu_download_auto_all()
        elif c=='3': 
            d = load_history()
            print(d.tail(10).to_string(index=False) if not d.empty else "Empty")
        elif c=='4':
            download_specific_chapter()
        elif c=='5': break

if __name__ == "__main__":
    main()
