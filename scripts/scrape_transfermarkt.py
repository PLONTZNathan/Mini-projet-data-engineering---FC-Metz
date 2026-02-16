import requests
import time
import random
from bs4 import BeautifulSoup
import csv
from pathlib import Path
import re
from typing import Dict

# CONFIGURATION

BASE_URL = "https://www.transfermarkt.com"
LEAGUE_CODE = "FR1"
SEASON_ID = 2025

DATA_DIR = Path("data/raw/transfermarkt")
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

# UTILITY FUNCTIONS

def fetch_url(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"  [WARNING] Attempt {attempt} failed for {url}: {e}")
            time.sleep(1 + random.random() * 2)
    print(f"  [ERROR] Failed to fetch {url} after {MAX_RETRIES} attempts")
    return None

def random_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))

def convert_height_to_cm(height_str: str) -> int:
    if not height_str:
        return 0
    match = re.search(r'(\d+),(\d+)', height_str)
    if match:
        return int(match.group(1)) * 100 + int(match.group(2))
    return 0


# ==============================
# LEAGUE & CLUB SCRAPING & CLUB DETAILS EXTRACTION
# ==============================

def fetch_league_page(league_code, season_id):
    url = f"{BASE_URL}/ligue-1/startseite/wettbewerb/{league_code}/plus/?saison_id={season_id}"
    return fetch_url(url)

def parse_club_links(html, league, season):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="items")
    clubs = []
    if table:
        tbody = table.find("tbody")
        rows = tbody.find_all("tr", recursive=False)
        for row in rows:
            link_cell = row.find("td", class_="zentriert no-border-rechts")
            if link_cell and link_cell.a:
                club_name = link_cell.a["title"]
                club_link = f"{BASE_URL}{link_cell.a['href']}"
                
                print(f"  Extracting details for {club_name}...")
                club_details = extract_club_details(club_link)
                
                clubs.append({
                    "team": club_name,
                    "link": club_link,
                    "league": league,
                    "season": season,
                    "squad_size": club_details.get('squad_size', 0),
                    "average_age": club_details.get('average_age', 0.0),
                    "national_team_players": club_details.get('national_team_players', 0),
                    "stadium_name": club_details.get('stadium_name', ''),
                    "stadium_capacity": club_details.get('stadium_capacity', 0),
                    "table_position": club_details.get('table_position', 0),
                    "years_in_league": club_details.get('years_in_league', 0)
                })
                
                random_delay(1, 2)
    
    return clubs


def extract_club_details(club_url):
    """Extract detailed club information"""
    html = fetch_url(club_url)
    if not html:
        return {}
    
    soup = BeautifulSoup(html, "html.parser")
    
    club_data = {
        'squad_size': 0,
        'average_age': 0.0,
        'national_team_players': 0,
        'stadium_name': None,
        'stadium_capacity': 0,
        'table_position': 0,
        'years_in_league': 0
    }
    
    # Extract data from data-header__items (Squad size, Average age, National team players, Stadium)
    items = soup.find_all('li', class_='data-header__label')
    
    for item in items:
        label_text = item.get_text(strip=True)
        content = item.find('span', class_='data-header__content')
        
        if not content:
            continue
        
        if 'Squad size:' in label_text:
            squad_text = content.get_text(strip=True)
            if squad_text.isdigit():
                club_data['squad_size'] = int(squad_text)
        
        elif 'Average age:' in label_text:
            age_text = content.get_text(strip=True)
            try:
                club_data['average_age'] = float(age_text)
            except ValueError:
                pass
        
        elif 'National team players:' in label_text:
            # Extract just the number, ignore the link
            nat_link = content.find('a')
            if nat_link:
                nat_text = nat_link.get_text(strip=True)
                if nat_text.isdigit():
                    club_data['national_team_players'] = int(nat_text)
        
        elif 'Stadium:' in label_text:
            # Extract stadium name from the <a> link
            stadium_link = content.find('a')
            if stadium_link:
                club_data['stadium_name'] = stadium_link.get_text(strip=True)
            
            # Extract capacity from the span with tabellenplatz class
            capacity_span = content.find('span', class_='tabellenplatz')
            if capacity_span:
                capacity_text = capacity_span.get_text(strip=True)
                # Format: "48.583 Seats"
                match = re.search(r'([\d.,]+)\s+Seats', capacity_text)
                if match:
                    capacity_str = match.group(1).replace('.', '').replace(',', '')
                    try:
                        club_data['stadium_capacity'] = int(capacity_str)
                    except ValueError:
                        pass
    
    # Extract Table position and Years in league from data-header__club-info
    club_info_div = soup.find('div', class_='data-header__club-info')
    if club_info_div:
        labels = club_info_div.find_all('span', class_='data-header__label')
        
        for label in labels:
            label_text = label.get_text(strip=True)
            content = label.find('span', class_='data-header__content')
            
            if not content:
                continue
            
            if 'Table position:' in label_text:
                # Extract from the <a> link
                pos_link = content.find('a')
                if pos_link:
                    pos_text = pos_link.get_text(strip=True)
                    if pos_text.isdigit():
                        club_data['table_position'] = int(pos_text)
            
            elif 'In league since:' in label_text:
                # Extract from the <a> link
                years_link = content.find('a')
                if years_link:
                    years_text = years_link.get_text(strip=True)
                    # Format: "52 years"
                    match = re.search(r'(\d+)\s+year', years_text)
                    if match:
                        club_data['years_in_league'] = int(match.group(1))
    
    return club_data


def save_clubs_to_csv(clubs, filename):
    filepath = DATA_DIR / filename
    fieldnames = [
        "team", "link", "league", "season", "squad_size", "average_age",
        "national_team_players", "stadium_name", "stadium_capacity", 
        "table_position", "years_in_league"
    ]
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clubs)

def get_club_links_from_csv(filename):
    filepath = DATA_DIR / filename
    links = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            links.append(row["link"])
    return links


# ==============================
# PLAYER LIST SCRAPING
# ==============================

def get_players_from_club(club_url):
    html = fetch_url(club_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="items")
    players = []
    if table:
        tbody = table.find("tbody")
        rows = tbody.find_all("tr", recursive=False)
        for row in rows:
            player_cell = row.find("td", class_="hauptlink")
            if player_cell and player_cell.a:
                player_name = player_cell.a.text.strip()
                player_href = player_cell.a["href"]
                player_link = f"{BASE_URL}{player_href}"
                player_id = player_href.split("/")[-1]
                players.append({"name": player_name, "id": player_id, "link": player_link})
    return players

# ==============================
# PLAYER STATS EXTRACTION
# ==============================

def extract_profile_data(html_content: str):
    soup = BeautifulSoup(html_content, 'html.parser')
    profile_data = {'shirt_number': 0, 'preferred_foot': None}
    shirt_number_span = soup.find('span', class_='data-header__shirt-number')
    if shirt_number_span:
        match = re.search(r'#(\d+)', shirt_number_span.get_text(strip=True))
        if match:
            profile_data['shirt_number'] = int(match.group(1))
    info_cells = soup.find_all('span', class_='info-table__content')
    for i, cell in enumerate(info_cells):
        if 'Foot:' in cell.get_text(strip=True):
            if i + 1 < len(info_cells):
                profile_data['preferred_foot'] = info_cells[i + 1].get_text(strip=True)
                break
    return profile_data

def extract_player_stats(html_content: str, player_name: str):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {
        'name': player_name, 'age': 0, 'birth_date': None, 'birth_place': None,
        'nationalities': None, 'height_cm': 0, 'preferred_foot': None, 'current_club': None,
        'position': None, 'shirt_number': 0, 'joined_date': None, 'contract_end': None,
        'market_value_m': 0, 'matches': 0, 'goals': 0, 'assists': 0,
        'minutes': 0, 'yellow_cards': 0, 'red_cards': 0
    }

    # Personal data
    header_items = soup.find_all('li', class_='data-header__label')
    for item in header_items:
        label_text = item.get_text(strip=True)
        if 'Date of birth' in label_text or 'Age' in label_text:
            birth_span = item.find('span', itemprop='birthDate')
            if birth_span:
                birth_text = birth_span.get_text(strip=True)
                match = re.search(r'(\d{2}/\d{2}/\d{4})\s*\((\d+)\)', birth_text)
                if match:
                    data['birth_date'] = match.group(1)
                    data['age'] = int(match.group(2))
        elif 'Place of birth' in label_text:
            place_span = item.find('span', itemprop='birthPlace')
            if place_span:
                data['birth_place'] = place_span.get_text(strip=True)
        elif 'Citizenship' in label_text:
            nationality_span = item.find('span', itemprop='nationality')
            if nationality_span:
                data['nationalities'] = nationality_span.get_text(strip=True)
        elif 'Height' in label_text:
            height_span = item.find('span', itemprop='height')
            if height_span:
                data['height_cm'] = convert_height_to_cm(height_span.get_text(strip=True))
        elif 'Position' in label_text:
            position_span = item.find('span', class_='data-header__content')
            if position_span:
                data['position'] = position_span.get_text(strip=True)

    # Club
    club_header = soup.find('span', class_='data-header__club')
    if club_header:
        club_link = club_header.find('a')
        if club_link:
            data['current_club'] = club_link.get_text(strip=True)

    # Contract
    contract_labels = soup.find_all('span', class_='data-header__label')
    for label in contract_labels:
        text = label.get_text(strip=True)
        if 'Joined:' in text:
            match = re.search(r'Joined:\s*(\d{2}/\d{2}/\d{4})', text)
            if match:
                data['joined_date'] = match.group(1)
        elif 'Contract expires:' in text:
            match = re.search(r'Contract expires:\s*(\d{2}/\d{2}/\d{4})', text)
            if match:
                data['contract_end'] = match.group(1)

    # Market value
    market_value_div = soup.find('div', class_='data-header__box--small')
    if market_value_div:
        mv_link = market_value_div.find('a')
        if mv_link:
            mv_text = mv_link.get_text(strip=True)
            match_m = re.search(r'([\d.]+)\s*m', mv_text, re.IGNORECASE)
            if match_m:
                data['market_value_m'] = float(match_m.group(1))
            else:
                match_k = re.search(r'([\d.]+)\s*K', mv_text, re.IGNORECASE)
                if match_k:
                    data['market_value_m'] = float(match_k.group(1)) / 1000

    # Season stats 
    tables = soup.find_all('table', class_='items')
    for table in tables:
        # First search in tfoot
        tfoot = table.find('tfoot')
        footer_row = tfoot.find('tr') if tfoot else None
        
        # If not found in tfoot, search in tbody
        if not footer_row:
            tbody = table.find('tbody')
            if tbody:
                # Search for row with bg_blau_20 class
                footer_row = tbody.find('tr', class_='bg_blau_20')
                
                # Search for row containing "Insgesamt" or "Total"
                if not footer_row:
                    all_rows = tbody.find_all('tr')
                    for row in all_rows:
                        first_cell = row.find('td')
                        if first_cell and ('Insgesamt' in first_cell.get_text(strip=True) or 'Total' in first_cell.get_text(strip=True)):
                            footer_row = row
                            break
                
                # Fallback: first row
                if not footer_row:
                    rows = tbody.find_all('tr', class_='odd') + tbody.find_all('tr', class_='even')
                    if rows:
                        footer_row = rows[0]
        
        if footer_row:
            cells = footer_row.find_all('td')
            
            # Verify if it's the total row (with "Insgesamt")
            is_total_row = cells and ('Insgesamt' in cells[0].get_text(strip=True) or 'Total' in cells[0].get_text(strip=True))
            is_goalkeeper = data['position'] and 'Goalkeeper' in data['position']
            
            if len(cells) >= 8:
                try:
                    # Find starting index after "Insgesamt" and hidden cell
                    start_index = 0
                    if is_total_row:
                        for idx, cell in enumerate(cells):
                            if 'hide' in cell.get('class', []):
                                start_index = idx + 1
                                break
                    
                    # Extract statistics from correct index
                    data_cells = cells[start_index:]
                    
                    if is_goalkeeper:
                        # Structure for goalkeeper: [matches, goals, yellows, second_yellow, reds, goals_conceded, clean_sheet, minutes]
                        if len(data_cells) >= 6:
                            if data_cells[0].get_text(strip=True).isdigit():
                                data['matches'] = int(data_cells[0].get_text(strip=True))
                            if data_cells[1].get_text(strip=True).isdigit():
                                data['goals'] = int(data_cells[1].get_text(strip=True))
                            data['assists'] = 0  # Goalkeepers don't have assists
                            if data_cells[2].get_text(strip=True).isdigit():
                                data['yellow_cards'] = int(data_cells[2].get_text(strip=True))
                            if len(data_cells) > 4 and data_cells[4].get_text(strip=True).isdigit():
                                data['red_cards'] = int(data_cells[4].get_text(strip=True))
                            if len(data_cells) > 6:
                                minutes_text = data_cells[-1].get_text(strip=True).replace("'", "").replace(".", "").replace(",", "").strip()
                                if minutes_text.isdigit():
                                    data['minutes'] = int(minutes_text)
                    else:
                        # Structure for field player: [matches, goals, assists, yellows, -, -, minutes]
                        if len(data_cells) >= 6:
                            if data_cells[0].get_text(strip=True).isdigit():
                                data['matches'] = int(data_cells[0].get_text(strip=True))
                            if data_cells[1].get_text(strip=True).isdigit():
                                data['goals'] = int(data_cells[1].get_text(strip=True))
                            if data_cells[2].get_text(strip=True).isdigit():
                                data['assists'] = int(data_cells[2].get_text(strip=True))
                            if data_cells[3].get_text(strip=True).isdigit():
                                data['yellow_cards'] = int(data_cells[3].get_text(strip=True))
                            if len(data_cells) > 5 and data_cells[5].get_text(strip=True).isdigit():
                                data['red_cards'] = int(data_cells[5].get_text(strip=True))
                            if len(data_cells) > 6:
                                minutes_text = data_cells[-1].get_text(strip=True).replace("'", "").replace(".", "").replace(",", "").strip()
                                if minutes_text.isdigit():
                                    data['minutes'] = int(minutes_text)
                    break
                except Exception as e:
                    print(f"    [ERROR] Stats extraction: {e}")
                    continue
    
    return data

def scrape_player_details(player_id: str, player_name: str, profile_url: str) -> Dict:
    stats_url = profile_url.replace('/profil/', '/leistungsdaten/')
    profile_html = fetch_url(profile_url)
    stats_html = fetch_url(stats_url)
    if not profile_html or not stats_html:
        print(f"  [FAILED] {player_name} ({player_id})")
        return None
    profile_data = extract_profile_data(profile_html)
    data = extract_player_stats(stats_html, player_name)
    data['shirt_number'] = profile_data['shirt_number']
    data['preferred_foot'] = profile_data['preferred_foot']
    data['id'] = player_id
    data['link'] = profile_url
    return data

# ==============================
# SCRAPING ALL PLAYERS
# ==============================

def append_player_to_csv(filepath, player_data, fieldnames):
    """Append a single player to CSV (or create file if it doesn't exist)"""
    file_exists = filepath.exists()
    
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(player_data)

def scrape_all_players_with_stats(clubs_filename=f"ligue1_clubs_{SEASON_ID}.csv", limit=None, force_rescrape=False):
    """
    Scrape all players with stats from Ligue 1 clubs
    
    Args:
        clubs_filename: CSV file containing club links
        limit: Optional limit for testing (scrape only first N players)
        force_rescrape: If True, will re-scrape all players (file already deleted in main)
                       If False, skip players already in the CSV (resumable)
    """
    start_time = time.time()
    club_links = get_club_links_from_csv(clubs_filename)
    all_players = []
    seen_ids = set()
    
    output_file = DATA_DIR / f"ligue1_players_{SEASON_ID}_complete.csv"
    fieldnames = [
        'id', 'name', 'link', 'age', 'birth_date', 'birth_place', 
        'nationalities', 'height_cm', 'preferred_foot', 'current_club', 'position', 
        'shirt_number', 'joined_date', 'contract_end', 'market_value_m',
        'matches', 'goals', 'assists', 'minutes', 'yellow_cards', 'red_cards'
    ]
    
    # Load already scraped players (will be empty if force_rescrape deleted the file)
    already_scraped = set()
    if output_file.exists():
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_scraped.add(row['id'])
        print(f"\n[INFO] Found {len(already_scraped)} already scraped players")
    
    print("\nStep 1: Fetching player lists...")
    for club_url in club_links:
        print(f"  {club_url}")
        players = get_players_from_club(club_url)
        for player in players:
            if player["id"] not in seen_ids:
                seen_ids.add(player["id"])
                all_players.append(player)
        random_delay(1,2)
    
    print(f"  {len(all_players)} players found")
    
    # Limit if requested
    if limit:
        all_players = all_players[:limit]
        print(f"  Limited to first {limit} players for testing")
    
    # Filter out already scraped players
    players_to_scrape = [p for p in all_players if p['id'] not in already_scraped]
    
    if not players_to_scrape:
        print(f"\n[INFO] All {len(all_players)} players already scraped!")
        print(f"[INFO] Use --force to re-scrape everything")
        return
    
    print(f"\nStep 2: Scraping detailed stats for {len(players_to_scrape)} players...")
    print(f"  ({len(already_scraped)} already done, {len(players_to_scrape)} remaining)")
    
    success_count = 0
    fail_count = 0
    
    for i, player in enumerate(players_to_scrape, 1):
        total_progress = len(already_scraped) + i
        print(f"  [{total_progress}/{len(all_players)}] {player['name']}...", end=" ")
        
        player_data = scrape_player_details(player['id'], player['name'], player['link'])
        
        if player_data:
            # Save immediately to CSV
            append_player_to_csv(output_file, player_data, fieldnames)
            print("[OK - SAVED]")
            success_count += 1
        else:
            print("[FAILED]")
            fail_count += 1
        
        random_delay(2,3)
    
    elapsed = time.time() - start_time
    total_in_file = len(already_scraped) + success_count
    
    print(f"\n{'='*70}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*70}")
    print(f"Total players in file: {total_in_file}")
    print(f"  - Already scraped: {len(already_scraped)}")
    print(f"  - Newly scraped: {success_count}")
    print(f"  - Failed: {fail_count}")
    print(f"Output: {output_file}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"{'='*70}")

# ==============================
# MAIN
# ==============================

def main():
    import sys
    
    # Check for command line arguments
    force_rescrape = '--force' in sys.argv or '--force-rescrape' in sys.argv
    
    print("="*70)
    print("SCRAPING LIGUE 1 - TRANSFERMARKT (ALL PLAYERS)")
    print("="*70)
    
    if force_rescrape:
        print("\n  FORCE RESCRAPE MODE: Will re-scrape ALL data from scratch")
        # Delete all existing files
        clubs_file = DATA_DIR / f"ligue1_clubs_{SEASON_ID}.csv"
        players_file = DATA_DIR / f"ligue1_players_{SEASON_ID}_complete.csv"
        if clubs_file.exists():
            clubs_file.unlink()
            print(f"  Deleted: {clubs_file}")
        if players_file.exists():
            players_file.unlink()
            print(f"  Deleted: {players_file}")
    else:
        print("\n RESUMABLE MODE: Will skip already scraped players")
        print("  (Use --force to re-scrape everything)")
    
    print("\n1. Fetching league page...")
    html = fetch_league_page(LEAGUE_CODE, SEASON_ID)
    
    print("2. Parsing clubs with details...")
    clubs = parse_club_links(html, league=LEAGUE_CODE, season=SEASON_ID)
    save_clubs_to_csv(clubs, f"ligue1_clubs_{SEASON_ID}.csv")
    print(f"   {len(clubs)} clubs saved with detailed stats")
    
    print("\n3. Scraping players with stats...")
    scrape_all_players_with_stats(force_rescrape=force_rescrape)
    
    print("\n" + "="*70)
    print("Done.")
    print("="*70)

if __name__ == "__main__":
    main()