from fake_useragent import UserAgent
import os
from bs4 import BeautifulSoup
import requests
import csv
from tqdm import tqdm
import traceback
from multiprocessing import Pool
from numpy import array_split


def get_html(url):
    user_agent = UserAgent().chrome
    r = requests.get(url, headers={'User-Agent': user_agent})
    if r.ok:
        return r.text
    print(r.status_code)
    
def write_csv(data, file_name, order):
    with open(file_name, 'a') as file:
        writer = csv.DictWriter(file, fieldnames=order)
        is_empty = os.stat(file_name).st_size == 0
        if is_empty:
            writer.writeheader()
        writer.writerow(data)


def write_text_file(data, file_name):
    with open(file_name, 'a') as file:
        file.write(f'{data}, ')


def strip_parentheses(string):
    if string[0] == '[':
        return string[1:-1]
    return string


def get_event_info(soup):
    champ_title = strip_parentheses(
        soup.find('span', class_='LName').find('a').text)
    date = soup.find(attrs={'name': 'timeData'})['data-t']
    teams_titles = [
        [a.text for a in soup.select('span.sclassName a')][0],
        [a.text for a in soup.select('span.sclassName a')][1]]
    final_result = [int(div.text)for div in soup.select('div.score')]
    total_score = sum(final_result)
    status = soup.select('div#mScore')[0].select('span')[0].text.strip()
    if status == 'Cancel':
        return False
    else:
        first_half_result = soup.find('span', title="Score 1st Half").text
        second_half_result = soup.find('span', title="Score 2nd Half").text
        return {'champ_title': champ_title.strip(),
                'date': date.strip(),
                'home': teams_titles[0].strip(),
                'away': teams_titles[1].strip(),
                'result': '{}-{}'.format(final_result[0], final_result[1]),
                'total_score': total_score,
                'first_half': first_half_result,
                'second_half': second_half_result}


def get_stat_key_order(stats):
    keys = [ 
        'corner kicks home', 'corner kicks away', 'corner kicks(ht) home', 
        'corner kicks(ht) away', 'yellow cards home', 'yellow cards away', 
        'shots home', 'shots away', 'shots on goal home', 'shots on goal away', 
        'attacks home', 'attacks away', 'dangerous attacks home', 
        'dangerous attacks away', 'shots off goal home', 'shots off goal away', 
        'blocked home', 'blocked away', 'free kicks home', 'free kicks away', 
        'possession home', 'possession away', 'possession(ht) home', 'possession(ht) away', 
        'passes home', 'passes away', 'successful passes home', 'successful passes away', 
        'fouls home', 'fouls away', 'offsides home', 'offsides away', 'aerials home', 'aerials away', 
        'aerials won home', 'aerials won away', 'saves home', 'saves away', 'tackles home', 'tackles away', 
        'dribbles home', 'dribbles away', 'throw-ins home', 'throw-ins away', 'successful tackles home', 
        'successful tackles away', 'interceptions home', 'interceptions away', 'assists home', 'assists away',
        'substitutions home', 'substitutions away'
    ]
    
    data = {}

    for key in keys:
        data[key] = stats.get(key, None)
        
    return data
    

def get_event_stat(soup):
    try:
        stats_li = soup.find('ul', class_='stat').find_all('li')
        data = {}
        for li in stats_li:
            stat_row = li.find_all('span')
            home_score = stat_row[0].text.strip() or 0
            stat_title = stat_row[3].text.lower().strip()
            away_score = stat_row[6].text.strip() or 0
            data[f'{stat_title} home'] = home_score
            data[f'{stat_title} away'] = away_score
    except:
        return {}
    
    return get_stat_key_order(data)


def get_odds_stat(soup):
    trs = soup.select('tr')[1:]
    home_odds = []
    draw_odds = []
    away_odds = []
    for tr in trs:
        tds = tr.select('td')
        home_odds.append(float(tds[0].find('font')['data-o']))
        draw_odds.append(float(tds[1].find('font')['data-o']))
        away_odds.append(float(tds[2].find('font')['data-o']))
    
    return {
        'home_odds_start': home_odds[-1],
        'home_odds_final': home_odds[0],
        'home_odds_max': max(home_odds),
        'home_odds_min': min(home_odds),
        'draw_odds_start': draw_odds[-1],
        'draw_odds_final': draw_odds[0],
        'draw_odds_max': max(draw_odds),
        'draw_odds_min': min(draw_odds),
        'away_odds_start': away_odds[-1],
        'away_odds_final': away_odds[0],
        'away_odds_max': max(away_odds),
        'away_odds_min': min(away_odds)
    }
    
    
def get_odds_page_url(url):
    event_id = url.split('live-')[1]
    odds_page_template = 'https://www.nowgoal6.com/1x2/OddsHistory/{}?cid=177&company=Pinnacle'
    return odds_page_template.format(event_id)


def main(urls):
    for url in tqdm(urls):
        try:
            new_url = url.replace('h2h', 'live')
            stat_page_html = get_html(new_url)
            soup = BeautifulSoup(stat_page_html, 'lxml')
            event_info = get_event_info(soup)
            if event_info == False:
                continue
            event_stat = get_event_stat(soup)
            odds_page_html = get_html(get_odds_page_url(new_url))
            odds_info = get_odds_stat(BeautifulSoup(odds_page_html, 'lxml'))
            data = {**event_info, **event_stat, **odds_info}
            order = list(data.keys())
            write_csv(data, './ng_football_stats_scraper/data/euro_event_stats.csv', order)
        except Exception as e:
            print(traceback.format_exc())
            write_text_file(
                url, './ng_football_stats_scraper/urls/stat_failed_urls4.txt')
            continue


def start_parallel_execution(f, urls, n_proc):
    partial_args = array_split(urls, n_proc)
    with Pool(n_proc) as p:
        p.map(f, partial_args)


if __name__ == '__main__':
    urls_file = open('./ng_football_stats_scraper/urls/euro_events_urls.txt')
    urls = urls_file.read().split(', ')
    urls_file.close()
    start_parallel_execution(main, urls, 6)