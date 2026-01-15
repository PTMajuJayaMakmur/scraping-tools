import os
import re
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# Load environment variables
load_dotenv()

import time

class DramaboxScraper:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("DRAMABOX_API_KEY")
        self.base_url = "https://streamapi.web.id/api-dramabox/"
        self.history_file = "download_history.json"
        self.master_excel = "dramabox_master_list.xlsx"
        self.download_dir = "downloads"
        self.history = self._load_history()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
            "Cache-Control": "no-cache"
        }
        
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def _load_history(self) -> Dict[str, Any]:
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except:
                return {"downloaded_drama_ids": [], "downloaded_episode_ids": []}
        return {"downloaded_drama_ids": [], "downloaded_episode_ids": []}

    def _save_history(self):
        with open(self.history_file, 'w') as f:
            json.dump(self.history, f, indent=4)

    def _clean_text(self, text: Any) -> Any:
        if isinstance(text, str):
            # Remove illegal characters for Excel
            return re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]', '', text)
        return text

    def _get(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}.php"
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        
        for attempt in range(5): # Increased to 5 attempts
            try:
                response = requests.get(url, params=params, headers=self.headers, timeout=30)
                if response.status_code == 502:
                    print(f"502 Bad Gateway at {url}, retrying ({attempt + 1}/5)...")
                    time.sleep(3 * (attempt + 1)) # Slightly longer backoff
                    continue
                    
                response.raise_for_status()
                data = response.json()
                if data.get('success'):
                    return data.get('data') or data
                else:
                    # Don't print error if it's just 'not found' type error, might be expected
                    if data.get('message') != "Data not found":
                        print(f"Error from API: {data.get('message')}")
                    return None
            except Exception as e:
                if attempt == 4:
                    print(f"Request failed after 5 attempts: {url} - {e}")
                else:
                    time.sleep(2 * (attempt + 1))
        return None

    def _post(self, endpoint: str, json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}.php"
        params = {'api_key': self.api_key}
        
        for attempt in range(5):
            try:
                response = requests.post(url, params=params, json=json_data, headers=self.headers, timeout=30)
                if response.status_code == 502:
                    print(f"502 Bad Gateway at {url} (POST), retrying ({attempt + 1}/5)...")
                    time.sleep(3 * (attempt + 1))
                    continue

                response.raise_for_status()
                data = response.json()
                if data.get('success'):
                    return data.get('data') or data
                else:
                    print(f"Error from API (POST): {data.get('message')}")
                    return None
            except Exception as e:
                if attempt == 4:
                    print(f"POST request failed after 5 attempts: {url} - {e}")
                else:
                    time.sleep(2 * (attempt + 1))
        return None

    def get_drama_list(self, page: int = 1, page_size: int = 10, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("new", {"page": page, "pageSize": page_size, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', []), data.get('isMore', False)
        return [], False

    def search_drama(self, query: str, page: int = 1, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("search", {"q": query, "page": page, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', []), data.get('isMore', False)
        return [], False

    def get_ranking(self, page: int = 1, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("rank", {"page": page, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', []), data.get('isMore', False)
        return [], False

    def get_foryou(self, page: int = 1, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("foryou", {"page": page, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', []), data.get('isMore', False)
        return [], False

    def get_classify(self, genre: str, sort: int = 1, page: int = 1, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("classify", {"genre": genre, "sort": sort, "pageNo": page, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', []), data.get('isMore', False)
        return [], False

    def get_suggest(self, query: str, lang: str = "in") -> List[Dict[str, Any]]:
        data = self._get("suggest", {"q": query, "lang": lang})
        if data:
            if isinstance(data, dict):
                return data.get('list', [])
        return []

    def get_chapters(self, drama_id: str, lang: str = "in") -> Optional[Dict[str, Any]]:
        """Fetch the chapter list and drama metadata from chapters.php."""
        data = self._get("chapters", {"bookId": drama_id, "lang": lang})
        if data:
            if isinstance(data, dict):
                # Ensure chapterList exists for consistency
                if 'list' in data and 'chapterList' not in data:
                    data['chapterList'] = data['list']
                return data
            elif isinstance(data, list):
                return {"bookId": drama_id, "chapterList": data, "list": data}
        return None

    def get_drama_detail(self, drama_id: str, lang: str = "in") -> Optional[Dict[str, Any]]:
        """
        drama.php is deprecated/404. Using chapters.php as the primary source.
        Returns a dict containing chapters and drama metadata.
        """
        return self.get_chapters(drama_id, lang)

    def get_watch_info(self, drama_id: str, index: int, lang: str = "in", chapter_id: str = None) -> Optional[Dict[str, Any]]:
        # Try watch.php first
        res = self._get("watch", {
            "bookId": drama_id, 
            "chapterIndex": index, 
            "lang": lang, 
            "source": "search_result",
            "raw": "true"
        })
        
        # If watch.php fails (502 or empty after retries) and we have chapter_id, try player.php
        if not res and chapter_id:
            print(f"Watch failed/unstable for index {index}, trying player.php fallback...")
            res = self.get_player_info(drama_id, chapter_id)
            
        return res

    def get_player_info(self, book_id: str, chapter_id: str) -> Optional[Dict[str, Any]]:
        # Example POST endpoint usage
        return self._post("player", {"bookId": book_id, "chapterId": chapter_id})

    def download_file(self, url: str, folder: str, filename: str) -> bool:
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        filepath = os.path.join(folder, filename)
        if os.path.exists(filepath):
            print(f"File already exists: {filepath}")
            return True
        
        try:
            print(f"Downloading: {filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"Download failed for {url}: {e}")
            return False

    def save_to_excel(self, dramas: List[Dict[str, Any]], filename: str = None):
        filename = filename or self.master_excel
        df = pd.DataFrame(dramas)
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")

    def update_master_excel(self, drama_info: Dict[str, Any]):
        """Update a single drama record in the master excel file."""
        # Clean data first
        for key, value in drama_info.items():
            drama_info[key] = self._clean_text(value)

        df = None
        if os.path.exists(self.master_excel):
            try:
                df = pd.read_excel(self.master_excel)
            except:
                pass
        
        required_columns = ["ID", "Title", "Introduction", "Tags", "Episodes Downloaded", "Total Episodes (API)", "Last Updated"]
        if df is None:
            df = pd.DataFrame(columns=required_columns)
        else:
            for col in required_columns:
                if col not in df.columns:
                    df[col] = "" # Add missing column

        # Ensure ID is string for comparison
        df['ID'] = df['ID'].astype(str)
        drama_id = str(drama_info['ID'])
        
        # Check if exists
        mask = df['ID'] == drama_id
        if mask.any():
            for key, value in drama_info.items():
                if key in df.columns:
                    df.loc[mask, key] = value
            df.loc[mask, "Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            new_row = drama_info.copy()
            new_row["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        df.to_excel(self.master_excel, index=False)

    def get_excel_history(self) -> Dict[str, int]:
        """Returns a mapping of Drama ID to Episodes Downloaded from Excel."""
        if not os.path.exists(self.master_excel):
            return {}
        try:
            df = pd.read_excel(self.master_excel)
            df['ID'] = df['ID'].astype(str)
            return dict(zip(df['ID'], df['Episodes Downloaded']))
        except:
            return {}

    def download_drama(self, drama_id: str, lang: str = "in", only_new: bool = False, drama_info: Dict[str, Any] = None):
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            # If we have basic info passed from download_all, use it as fallback
            if drama_info:
                detail = drama_info.copy()
                detail['chapterList'] = self.get_chapters(drama_id, lang).get('chapterList', [])
            else:
                return
        
        # Merge drama_info if provided (it might have bookName while detail doesn't)
        if drama_info:
            for k, v in drama_info.items():
                if k not in detail or not detail[k]:
                    detail[k] = v

        book_name = detail.get('bookName') or detail.get('title') or f"Drama_{drama_id}"
        drama_name = "".join(x for x in book_name if x.isalnum() or x in " -_").strip()
        drama_folder = os.path.join(self.download_dir, f"{drama_id}_{drama_name}")
        
        if not os.path.exists(drama_folder):
            os.makedirs(drama_folder)

        # Download Cover
        cover_url = detail.get('cover') or detail.get('bookCover')
        if cover_url:
            self.download_file(cover_url, drama_folder, "cover.jpg")

        episodes = detail.get('chapterList', [])
        if not episodes and 'list' in detail:
            episodes = detail['list']
            
        total_episodes = len(episodes)
        print(f"Found {total_episodes} episodes for '{book_name}'")

        new_episodes_found = False
        downloaded_count = 0
        
        # Count existing files first to be accurate
        for f in os.listdir(drama_folder):
            if f.startswith("episode_") and f.endswith(".mp4"):
                downloaded_count += 1

        for ep in episodes:
            ep_index = ep.get('chapterIndex')
            if ep_index is None: continue # Skip if no index
            
            ep_id = str(ep.get('chapterId', ''))
            
            if only_new and ep_id in self.history['downloaded_episode_ids']:
                continue

            watch_info = self.get_watch_info(drama_id, ep_index, lang, chapter_id=ep_id)
            if watch_info and watch_info.get('videoUrl'):
                filename = f"episode_{ep_index + 1}.mp4"
                success = self.download_file(watch_info['videoUrl'], drama_folder, filename)
                if success:
                    new_episodes_found = True
                    downloaded_count += 1
                    if ep_id and ep_id not in self.history['downloaded_episode_ids']:
                        self.history['downloaded_episode_ids'].append(ep_id)
        
        # Update Master Excel
        tags = detail.get('tags', [])
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        
        self.update_master_excel({
            "ID": drama_id,
            "Title": book_name,
            "Introduction": detail.get('introduction', detail.get('bookIntroduction', '')),
            "Tags": tags_str,
            "Episodes Downloaded": downloaded_count,
            "Total Episodes (API)": total_episodes
        })

        if new_episodes_found or drama_id not in self.history['downloaded_drama_ids']:
            if drama_id not in self.history['downloaded_drama_ids']:
                self.history['downloaded_drama_ids'].append(drama_id)
            self._save_history()
        else:
            print(f"No new episodes for '{book_name}'")


    def download_all(self, lang: str = "in", only_new: bool = False):
        excel_history = self.get_excel_history() if only_new else {}
        page = 1
        has_more = True
        
        while has_more:
            print(f"Fetching page {page}...")
            dramas, has_more = self.get_drama_list(page=page, lang=lang)
            if not dramas:
                break
            
            for drama in dramas:
                drama_id = str(drama['bookId'])
                api_ep_count = drama['chapterCount']
                
                if only_new and drama_id in excel_history:
                    downloaded_so_far = excel_history[drama_id]
                    if api_ep_count <= downloaded_so_far:
                        print(f"Skipping '{drama['bookName']}' (Already up to date: {downloaded_so_far} eps)")
                        continue
                    else:
                        print(f"Update found for '{drama['bookName']}': {downloaded_so_far} -> {api_ep_count} eps")

                print(f"Processing Drama: {drama['bookName']}")
                self.download_drama(drama_id, lang, only_new=only_new, drama_info=drama)
            
            page += 1
        print("Download process finished.")

    def download_single_episode(self, drama_id: str, episode_index: int, lang: str = "in"):
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            return
            
        drama_name = "".join(x for x in detail['bookName'] if x.isalnum() or x in " -_").strip()
        drama_folder = os.path.join(self.download_dir, f"{drama_id}_{drama_name}")
        
        watch_info = self.get_watch_info(drama_id, episode_index, lang)
        if watch_info and watch_info.get('videoUrl'):
            filename = f"episode_{episode_index + 1}.mp4"
            self.download_file(watch_info['videoUrl'], drama_folder, filename)
            
            # Record in history
            episodes = detail.get('chapterList', [])
            target_ep = next((e for e in episodes if e['chapterIndex'] == episode_index), None)
            if target_ep:
                ep_id = target_ep['chapterId']
                if ep_id not in self.history['downloaded_episode_ids']:
                    self.history['downloaded_episode_ids'].append(ep_id)
                    self._save_history()

    def sync_local_folders(self):
        """Scan folders and update the master excel file."""
        print(f"Scanning folder: {self.download_dir}...")
        if not os.path.exists(self.download_dir):
            print("Download folder not found.")
            return

        folders = [f for f in os.listdir(self.download_dir) if os.path.isdir(os.path.join(self.download_dir, f))]
        
        for folder_name in folders:
            if "_" in folder_name:
                parts = folder_name.split("_", 1)
                drama_id = parts[0]
                drama_title = parts[1]
            else:
                drama_id = "Unknown"
                drama_title = folder_name
            
            if drama_id == "Unknown": continue

            folder_path = os.path.join(self.download_dir, folder_name)
            ep_count = len([f for f in os.listdir(folder_path) if f.endswith(".mp4")])
            
            # Update Master Excel from local scan
            self.update_master_excel({
                "ID": drama_id,
                "Title": drama_title,
                "Episodes Downloaded": ep_count,
                "Total Episodes (API)": ep_count # We don't know API count from local scan
            })
            
            if drama_id not in self.history['downloaded_drama_ids']:
                self.history['downloaded_drama_ids'].append(drama_id)

        self._save_history()
        print(f"Sync complete. Check {self.master_excel}")

    def export_drama_to_excel_with_urls(self, drama_id: str, lang: str = "in"):
        """Fetch drama metadata and all episode URLs, then export to a separate Excel file."""
        print(f"Fetching details for Drama ID: {drama_id}...")
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            print(f"Failed to get details for Drama ID: {drama_id}")
            return
        
        drama_name = detail.get('bookName', f'Drama_{drama_id}')
        episodes = detail.get('chapterList', [])
        tags = detail.get('tags', [])
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        intro = detail.get('introduction', detail.get('bookIntroduction', ''))
        
        data_list = []
        print(f"Fetching video URLs for {len(episodes)} episodes... This may take a moment.")
        
        for ep in episodes:
            ep_index = ep['chapterIndex']
            ep_id = str(ep.get('chapterId', ''))
            print(f"  Fetching URL for Episode {ep_index + 1}/{len(episodes)}...", end='\r')
            watch_info = self.get_watch_info(drama_id, ep_index, lang, chapter_id=ep_id)
            
            data_list.append({
                "Drama ID": self._clean_text(drama_id),
                "Drama Title": self._clean_text(drama_name),
                "Introduction": self._clean_text(intro),
                "Tags": self._clean_text(tags_str),
                "Episode Index": ep_index + 1,
                "Episode Title": self._clean_text(ep.get('chapterName', f'Episode {ep_index + 1}')),
                "Video URL": watch_info.get('videoUrl', 'Not Found') if watch_info else 'Error'
            })
        
        print(f"\nFetched {len(data_list)} episode URLs.")
        
        # Save to a specific file for this drama
        safe_name = "".join(x for x in drama_name if x.isalnum() or x in " -_").strip()
        export_filename = f"drama_info_{drama_id}_{safe_name}.xlsx"
        
        df = pd.DataFrame(data_list)
        df.to_excel(export_filename, index=False)
        print(f"Detailed drama info with URLs saved to: {export_filename}")

    def export_all_dramas_to_excel_with_urls(self, lang: str = "in"):
        """Iterate through all pages and export every drama's info and episode URLs to one Excel."""
        page = 1
        has_more = True
        all_data_list = []
        
        print("Starting global export of all dramas with video URLs...")
        
        while has_more:
            print(f"\n--- Fetching Page {page} ---")
            dramas, has_more = self.get_drama_list(page=page, lang=lang)
            if not dramas:
                break
            
            for drama in dramas:
                drama_id = str(drama['bookId'])
                print(f"Processing Drama: {drama['bookName']} (ID: {drama_id})")
                
                detail = self.get_drama_detail(drama_id, lang)
                if not detail:
                    print(f"  Skipping ID {drama_id}: Could not fetch details.")
                    continue
                
                episodes = detail.get('chapterList', [])
                tags = detail.get('tags', [])
                tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
                intro = detail.get('introduction', detail.get('bookIntroduction', ''))
                
                for ep in episodes:
                    ep_index = ep['chapterIndex']
                    ep_id = str(ep.get('chapterId', ''))
                    print(f"    Fetching URL for Episode {ep_index + 1}/{len(episodes)}...", end='\r')
                    watch_info = self.get_watch_info(drama_id, ep_index, lang, chapter_id=ep_id)
                    
                    all_data_list.append({
                        "Drama ID": self._clean_text(drama_id),
                        "Drama Title": self._clean_text(detail.get('bookName', '')),
                        "Introduction": self._clean_text(intro),
                        "Tags": self._clean_text(tags_str),
                        "Episode Index": ep_index + 1,
                        "Episode Title": self._clean_text(ep.get('chapterName', f'Episode {ep_index + 1}')),
                        "Video URL": watch_info.get('videoUrl', 'Not Found') if watch_info else 'Error'
                    })
                print(f"\n  Done: {len(episodes)} episodes added.")
            
            page += 1
        
        if all_data_list:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_filename = f"dramabox_all_urls_{timestamp}.xlsx"
            print(f"\nSaving {len(all_data_list)} total rows to Excel...")
            df = pd.DataFrame(all_data_list)
            df.to_excel(export_filename, index=False)
            print(f"Global export complete: {export_filename}")
        else:
            print("No data found to export.")

def main():
    scraper = DramaboxScraper()
    
    while True:
        print("\n=== Dramabox Scraper Menu ===")
        print("1. Download All (Videos, Covers, Excel)")
        print("2. Download 1 Drama (All Episodes)")
        print("3. Download 1 Specific Episode")
        print("4. Check Update (Download only new items)")
        print("5. Sync Folders to Excel & Refresh History")
        print("6. Export ALL Dramas + Video URLs (Loop all pages)")
        print("7. Search Drama")
        print("8. Trending / Ranking")
        print("9. For You / Recommendations")
        print("10. Exit")
        
        choice = input("Enter choice (1-10): ")
        
        if choice == '1':
            scraper.download_all()
        elif choice == '2':
            drama_id = input("Enter Drama ID: ")
            scraper.download_drama(drama_id)
        elif choice == '3':
            drama_id = input("Enter Drama ID: ")
            ep_idx = input("Enter Episode Index (starting from 0): ")
            if ep_idx.isdigit():
                scraper.download_single_episode(drama_id, int(ep_idx))
        elif choice == '4':
            scraper.download_all(only_new=True)
        elif choice == '5':
            scraper.sync_local_folders()
        elif choice == '6':
            scraper.export_all_dramas_to_excel_with_urls()
        elif choice == '7':
            query = input("Enter search query: ")
            dramas, _ = scraper.search_drama(query)
            if dramas:
                print(f"\nResults for '{query}':")
                for d in dramas:
                    print(f"- [{d.get('bookId')}] {d.get('bookName')} ({d.get('chapterCount')} episodes)")
            else:
                print("No results found.")
        elif choice == '8':
            dramas, _ = scraper.get_ranking()
            if dramas:
                print("\nTrending / Ranking:")
                for d in dramas:
                    print(f"- [{d.get('bookId')}] {d.get('bookName')} ({d.get('chapterCount')} episodes)")
        elif choice == '9':
            dramas, _ = scraper.get_foryou()
            if dramas:
                print("\nFor You / Recommendations:")
                for d in dramas:
                    print(f"- [{d.get('bookId')}] {d.get('bookName')} ({d.get('chapterCount')} episodes)")
        elif choice == '10':
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
