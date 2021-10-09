import json
import requests as req
from datetime import datetime
import pandas as pd
import pytz

headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Authorization': '',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Origin': 'https://soundcloud.com',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://soundcloud.com/',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}

def ts_from_date(date):
    if type(date) is not datetime.date:
        date = pd.to_datetime(date)
    return int(datetime(date.year, date.month, date.day, tzinfo=pytz.utc).timestamp()) * 1000

def load_tracklist(limit=200, raw=False, headers=headers):
    url_params = {
        'representation': 'owner',
        'client_id': 'gNVzaqTohpqhemjfMYjlPlv14Nu4sn9u',
        'limit': limit,
        'offset': 0,
        'linked_partitioning': 1,
        'app_version': 1626178526,
        'app_locale': 'en',
        
    }
    
    url = 'https://api-v2.soundcloud.com/users/699032811/tracks'
    
    res = req.request("GET", url, params=url_params, headers=headers)
    
    raw_tracks = json.loads(res.text)['collection']
    
    if raw:
        return raw_tracks
    all_tracks = []
    
    keys = [
        'title',
        'id',
        'permalink',
        'permalink_url',
        'public',
        'playback_count',
        'likes_count',
        'genre',
        'tag_list',
        'display_date',
        'created_at',
        'duration',
    ]
    
    for track in raw_tracks:
        all_tracks.append({ k: track[k] for k in keys })

    return all_tracks



def load_track_stats(track, date_start, date_end, headers=headers):
    url = 'https://api-v2.soundcloud.com/users/soundcloud:users:699032811/tracks/soundcloud:tracks:{}/stats/timeseries/plays'.format(track['id'])
    
    ts_from = ts_from_date(date_start)
    ts_to = ts_from_date(date_end)
    
    url_params = {
        'from': ts_from,
        'to': ts_to,
        'resolution': 'day',
        'client_id': 'gNVzaqTohpqhemjfMYjlPlv14Nu4sn9u',
        'app_version': 1626178526,
        'app_locale': 'en',
    }
    
    res = req.request("GET", url, params=url_params, headers=headers)
    
    return json.loads(res.text)['timeseries']
    


def load_track_by_date_stats(limit=200, dump=False):
    tracks = load_tracklist(limit)
    tracks = [x for x in tracks if x['public'] is True]
    
    track_stats = pd.DataFrame()
    for i, track in enumerate(tracks):
        print('loading {} of {}: "{}"'.format(i+1, len(tracks), track['title']))
        date_start = pd.to_datetime(track['created_at']).date()
        date_end = datetime.now(pytz.timezone('UTC')).date()
        try:
            track_part = pd.DataFrame(load_track_stats(track, date_start, date_end))
            track_part['id'] = track['id']
            track_part['date'] = pd.to_datetime(track_part['time'], unit='ms')
            track_stats = track_stats.append(track_part, ignore_index=True)
        except:
            print('->Errored')
    if dump:
        track_stats.to_pickle('track_stats_{}.pkl'.format(datetime.now()))
    return track_stats
    

def load_platforms_for_track(track, date_start, date_end, headers=headers):
    url = 'https://api-v2.soundcloud.com/users/soundcloud:users:699032811/tracks/soundcloud:tracks:{}/stats/top-lists/plays/podcast-app'.format(track['id'])

    ts_from = ts_from_date(date_start)
    ts_to = ts_from_date(date_end)
    
    url_params = {
        'from': ts_from,
        'to': ts_to,
        'limit': 50,
        'offset': 0,
        'client_id': 'e6PnoBAyFVAG788KrTcgqLF0nv4ZaAtQ',
        'linked_partitioning': 1,
        'app_version': 1626178526,
        'app_locale': 'en',
    }
    
    res = req.request("GET", url, params=url_params, headers=headers)
    return json.loads(res.text)['counts']

def load_platforms_for_track(track, date_start, date_end, headers=headers):
    url = 'https://api-v2.soundcloud.com/users/soundcloud:users:699032811/tracks/soundcloud:tracks:{}/stats/top-lists/plays/podcast-app'.format(track['id'])

    ts_from = ts_from_date(date_start)
    ts_to = ts_from_date(date_end)
    
    url_params = {
        'from': ts_from,
        'to': ts_to,
        'limit': 50,
        'offset': 0,
        'client_id': 'e6PnoBAyFVAG788KrTcgqLF0nv4ZaAtQ',
        'linked_partitioning': 1,
        'app_version': 1626178526,
        'app_locale': 'en',
    }
    
    res = req.request("GET", url, params=url_params, headers=headers)
    return json.loads(res.text)['counts']

def load_track_total(track, date_start, date_end):
    url = 'https://api-v2.soundcloud.com/users/soundcloud:users:699032811/tracks/soundcloud:tracks:{}/stats/totals'.format(track['id'])
    
    ts_from = ts_from_date(date_start)
    ts_to = ts_from_date(date_end)
    
    url_params = {
        'from': ts_from,
        'to': ts_to,
        'client_id': 'e6PnoBAyFVAG788KrTcgqLF0nv4ZaAtQ',
        'app_version': 1626178526,
        'app_locale': 'en',
    }
    
    res = req.request("GET", url, params=url_params, headers=headers)

    return json.loads(res.text)['period']['plays']
   
    
def load_sc_plays_for_track(track, date_start, date_end):
    
    ts_from = ts_from_date(date_start)
    ts_to = ts_from_date(date_end)
    
    url = 'https://api-v2.soundcloud.com/users/soundcloud:users:699032811/tracks/soundcloud:tracks:{}/stats/top-lists/plays/soundcloud-app'.format(track['id'])
    
    url_params = {
        'from': ts_from,
        'to': ts_to,
        'client_id': 'e6PnoBAyFVAG788KrTcgqLF0nv4ZaAtQ',
        'limit': 50,
        'offset': 0,
        'linked_partitioning': 1,
        'app_version': 1626178526,
        'app_locale': 'en',
    }
    
    res = req.request("GET", url, params=url_params, headers=headers)

    plays = json.loads(res.text)['counts']
    
    ret_plays = []
    
    for each in plays:
        ret_plays.append({
            'subject': each['subject']['name'],
            'count': each['count'],
            'type': 'soundcloud',
        })

    return ret_plays
    

def get_all_stats(track, date_start, date_end):
    total = load_track_total(track, date_start, date_end)
    if (total == 0):
        raise(Exception('Empty date stats'))
        
    df_slice = pd.DataFrame(load_platforms_for_track(track, date_start, date_end))
    df_slice['type'] = 'rss'
    df_slice = df_slice.append(pd.DataFrame(load_sc_plays_for_track(track, date_start, date_end)))
    
    df_slice.rename(columns={'subject': 'platform'}, inplace=True)
    df_slice = df_slice.sort_values(by=['count'], ascending=False)[['platform', 'type', 'count']]
    
    surplus = {
        'type': 'rss',
        'platform': 'unknown',
        'count': total - df_slice['count'].sum(),
    }
    
    df_slice = df_slice.append(surplus, ignore_index=True)
    
    df_slice['date_start'], df_slice['date_end'] = date_start, date_end
    

    
    return df_slice


def load_stats_by_platfrom(limit, dump=False):
    tracks = load_tracklist(limit)
    tracks = [x for x in tracks if x['public'] is True]
    
    track_stats = pd.DataFrame()
    for i, track in enumerate(tracks):
        print('loading {} of {}: "{}"'.format(i+1, len(tracks), track['title']))
        date_start = pd.to_datetime(track['created_at']).date()
        date_end = datetime.now(pytz.timezone('UTC')).date()
        for day in pd.date_range(date_start, date_end):
            try:
                day = day.date()
                track_part = pd.DataFrame(get_all_stats(track, day, day))
                track_part['track_id'] = track['id']
                track_part['track_title'] = track['title']
                track_part['date'] = str(day)
                track_stats = track_stats.append(track_part, ignore_index=True)
            except Exception as e:
                if str(e) == 'Empty date stats':
                    print('{}:->Empty date'.format(day))
                else:
                    print('{}:->Errored'.format(day))
            else:
                print('{}:->Collected'.format(day))
    ordered_columns = [
        'track_id',
        'track_title',
        'type',
        'date',
        'platform',
        'count',
    ]
    
    track_stats = track_stats[ordered_columns]
    if dump:
        track_stats.to_pickle('track_stats_by_platform_{}.pkl'.format(datetime.now()))
    return track_stats
    
    
    
    
    
    
    