import requests
import json
import time

# --- CONFIGURATION START ---
# Anda bisa menyembunyikan atau mengenkripsi bagian ini nantinya
# Misalnya load dari environment variable atau file config terpisah
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # "Accept": "application/json",
    # Tidak menggunakan header auth khusus karena endpoint ini tampaknya publik atau hanya butuh UA
}

BASE_URL = "https://streamapi.web.id/api-dramabox/index.php"
# --- CONFIGURATION END ---

def fetch_dramas(page=1, lang="in"):
    """
    Mengambil data drama dari API berdasarkan halaman dan bahasa.
    """
    params = {
        "page": page,
        "lang": lang
    }
    
    print(f"[*] Fetching page {page} from {BASE_URL}...")
    
    try:
        response = requests.get(BASE_URL, headers=API_HEADERS, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Validasi struktur response dasar sesuai yang diminta user
        if data.get("status") is True:
            return data
        else:
            print(f"[!] API Error: Status is not true. Response: {data}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"[!] Connection Error: {e}")
        return None
    except json.JSONDecodeError:
        print("[!] Error: Failed to decode JSON response")
        return None

def process_drama_list(data):
    """
    Memproses dan menampilkan daftar drama.
    """
    if not data or "data" not in data:
        print("[!] No data found.")
        return

    print(f"\n[+] Total Page: {data.get('total_page')}")
    print(f"[+] Total Data: {data.get('total_data')}")
    print("-" * 50)
    
    for drama in data["data"]:
        print(f"ID       : {drama.get('id')}")
        print(f"Title    : {drama.get('title')}")
        print(f"Episode  : {drama.get('episode')}")
        print(f"Thumbnail: {drama.get('thumbnail')}")
        print(f"URL Details: {drama.get('url')}")
        print("-" * 30)

def main():
    # Contoh penggunaan untuk mengambil halaman 1
    result = fetch_dramas(page=1)
    
    if result:
        # Tampilkan pretty print JSON seperti diminta user
        # print(json.dumps(result, indent=4))
        process_drama_list(result)

if __name__ == "__main__":
    main()
