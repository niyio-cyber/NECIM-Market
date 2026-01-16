#!/usr/bin/env python3
"""
NECMIS Production Scraper v3.0
Northeast Construction Market Intelligence System

Sources (all verified current as of Jan 2025):
- 14 RSS feeds (regional news + industry publications)
- 8 DOT state pages

Lines of Business Covered:
- Highway/Road Construction
- Hot Mix Asphalt
- Aggregates (Sand, Gravel, Stone)
- Ready Mix Concrete
- Liquid Asphalt/Bitumen
- Trucking/Hauling
"""

import requests
import json
import re
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import xml.etree.ElementTree as ET
import hashlib
from urllib.parse import urlparse
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

USER_AGENT = "NECMIS/3.0 (Construction Market Intelligence)"
TIMEOUT = 30
DAYS_BACK = 14

STATES = ['VT', 'NH', 'ME', 'NY', 'PA', 'MA', 'RI', 'CT']

# ============================================================================
# KEYWORDS BY BUSINESS LINE
# ============================================================================

KEYWORDS_HIGHWAY = [
    'highway', 'road construction', 'paving', 'resurfacing', 'milling',
    'interstate', 'turnpike', 'dot ', 'transportation', 'vtrans', 'nhdot',
    'mainedot', 'nysdot', 'penndot', 'massdot', 'ridot', 'ctdot',
    'bridge', 'overpass', 'culvert', 'guardrail', 'pavement'
]

KEYWORDS_HMA = [
    'asphalt', 'hot mix', 'hma', 'bituminous', 'blacktop', 'tack coat',
    'wearing course', 'overlay', 'asphalt plant', 'paver', 'roller'
]

KEYWORDS_AGGREGATES = [
    'aggregate', 'quarry', 'gravel', 'sand', 'stone', 'crushed',
    'pit', 'mining', 'excavation', 'crusher', 'screening',
    'base course', 'subbase', 'fill material'
]

KEYWORDS_CONCRETE = [
    'concrete', 'ready mix', 'ready-mix', 'cement', 'batch plant',
    'redi-mix', 'precast', 'reinforced concrete', 'structural concrete'
]

KEYWORDS_BITUMEN = [
    'bitumen', 'liquid asphalt', 'asphalt cement', 'emulsion',
    'ac grade', 'pg grade', 'binder', 'cutback', 'asphalt terminal'
]

KEYWORDS_TRUCKING = [
    'trucking', 'hauling', 'dump truck', 'fleet', 'cdl',
    'freight', 'delivery', 'transport', 'logistics'
]

KEYWORDS_OPPORTUNITY = [
    'bid', 'rfp', 'rfq', 'letting', 'proposal', 'solicitation',
    'contract', 'award', 'procurement', 'advertised'
]

KEYWORDS_FUNDING = [
    'grant', 'funding', 'iija', 'federal', 'appropriation',
    'infrastructure bill', 'million', 'billion', 'budget'
]

ALL_KEYWORDS_HIGH = (KEYWORDS_HIGHWAY + KEYWORDS_HMA + KEYWORDS_OPPORTUNITY + 
                     KEYWORDS_FUNDING[:5])
ALL_KEYWORDS_MEDIUM = (KEYWORDS_AGGREGATES + KEYWORDS_CONCRETE + 
                       KEYWORDS_BITUMEN + KEYWORDS_TRUCKING)

STATE_PATTERNS = {
    'VT': ['vermont', 'vtrans', 'burlington', 'montpelier', 'rutland', 'bennington', 'brattleboro', 'barre'],
    'NH': ['new hampshire', ' nh ', 'nhdot', 'manchester', 'nashua', 'concord', 'portsmouth', 'keene', 'laconia'],
    'ME': ['maine', 'mainedot', 'portland me', 'bangor', 'lewiston', 'augusta', 'presque isle', 'biddeford'],
    'NY': ['new york', 'nysdot', 'albany', 'syracuse', 'rochester', 'buffalo', 'thruway', 'utica', 'binghamton'],
    'PA': ['pennsylvania', 'penndot', 'harrisburg', 'pittsburgh', 'philadelphia', 'turnpike', 'scranton', 'allentown'],
    'MA': ['massachusetts', 'massdot', 'boston', 'worcester', 'springfield', 'mass pike', 'cambridge', 'lowell'],
    'RI': ['rhode island', 'ridot', 'providence', 'warwick', 'cranston', 'newport', 'pawtucket'],
    'CT': ['connecticut', 'ctdot', 'hartford', 'new haven', 'bridgeport', 'stamford', 'waterbury', 'norwalk']
}

# ============================================================================
# RSS FEED SOURCES - All verified current Jan 2025
# ============================================================================

RSS_FEEDS = {
    # Vermont
    'VTDigger': {'url': 'https://vtdigger.org/feed/', 'state': 'VT'},
    'Vermont Biz': {'url': 'https://vermontbiz.com/feed/', 'state': 'VT'},
    
    # New Hampshire  
    'Union Leader': {'url': 'https://www.unionleader.com/search/?f=rss&t=article&c=news/business&l=50&s=start_time&sd=desc', 'state': 'NH'},
    'InDepthNH': {'url': 'https://indepthnh.org/feed/', 'state': 'NH'},
    
    # Maine
    'Press Herald': {'url': 'https://www.pressherald.com/feed/', 'state': 'ME'},
    'Bangor Daily': {'url': 'https://bangordailynews.com/feed/', 'state': 'ME'},
    
    # New York
    'Times Union': {'url': 'https://www.timesunion.com/rss/feed/News-702.php', 'state': 'NY'},
    'Syracuse.com': {'url': 'https://www.syracuse.com/arc/outboundfeeds/rss/?outputType=xml', 'state': 'NY'},
    
    # Pennsylvania
    'PennLive': {'url': 'https://www.pennlive.com/arc/outboundfeeds/rss/?outputType=xml', 'state': 'PA'},
    
    # Massachusetts
    'MassLive': {'url': 'https://www.masslive.com/arc/outboundfeeds/rss/?outputType=xml', 'state': 'MA'},
    
    # Rhode Island
    'Providence Journal': {'url': 'https://www.providencejournal.com/arcio/rss/category/news/', 'state': 'RI'},
    
    # Connecticut
    'CT Mirror': {'url': 'https://ctmirror.org/feed/', 'state': 'CT'},
    
    # Industry Publications (verified current - aggregates/concrete coverage)
    'Pit & Quarry': {'url': 'https://www.pitandquarry.com/feed/', 'state': 'ALL'},
    'ForConstructionPros': {'url': 'https://www.forconstructionpros.com/rss', 'state': 'ALL'},
}

# ============================================================================
# DOT PAGE SOURCES
# ============================================================================

DOT_PAGES = {
    'VTrans Construction': {
        'url': 'https://vtrans.vermont.gov/about/construction-report',
        'state': 'VT', 'type': 'projects'
    },
    'VTrans Bids': {
        'url': 'https://vtrans.vermont.gov/contract-admin/bids-requests/construction-contracting',
        'state': 'VT', 'type': 'bids'
    },
    'NHDOT Bids': {
        'url': 'https://www.dot.nh.gov/doing-business-nhdot/contractors/invitation-bid',
        'state': 'NH', 'type': 'bids'
    },
    'MaineDOT Projects': {
        'url': 'https://www.maine.gov/dot/projects/',
        'state': 'ME', 'type': 'projects'
    },
    'NYSDOT Lettings': {
        'url': 'https://www.dot.ny.gov/doing-business/opportunities/const-highway',
        'state': 'NY', 'type': 'bids'
    },
    'MassDOT Bids': {
        'url': 'https://www.mass.gov/info-details/advertised-projects-bid-opening-schedule',
        'state': 'MA', 'type': 'bids'
    },
    'RIDOT Projects': {
        'url': 'https://www.dot.ri.gov/projects/',
        'state': 'RI', 'type': 'projects'
    },
    'CTDOT Projects': {
        'url': 'https://portal.ct.gov/dot/projects/projects/projects-and-studies',
        'state': 'CT', 'type': 'projects'
    },
}

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def fetch_url(url: str, source: str) -> Optional[str]:
    try:
        headers = {
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except requests.exceptions.Timeout:
        print(f"    ⚠ Timeout: {source}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"    ⚠ HTTP {e.response.status_code}: {source}")
        return None
    except Exception as e:
        print(f"    ⚠ Error: {source} - {type(e).__name__}")
        return None


def parse_rss(xml_content: str, source: str, default_state: str) -> List[Dict]:
    items = []
    try:
        root = ET.fromstring(xml_content)
        
        for item in root.findall('.//item'):
            title = item.findtext('title', '').strip()
            link = item.findtext('link', '').strip()
            desc = item.findtext('description', '')
            pub_date = item.findtext('pubDate', '')
            
            if title and link:
                items.append({
                    'title': clean_html(title),
                    'url': link,
                    'summary': clean_html(desc)[:300],
                    'date': parse_date(pub_date),
                    'source': source,
                    'source_type': 'news',
                    'state': default_state
                })
        
        if not items:
            for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                title = entry.findtext('{http://www.w3.org/2005/Atom}title', '').strip()
                link_el = entry.find('{http://www.w3.org/2005/Atom}link')
                link = link_el.get('href', '') if link_el is not None else ''
                summary = entry.findtext('{http://www.w3.org/2005/Atom}summary', '')
                published = entry.findtext('{http://www.w3.org/2005/Atom}published', '')
                
                if title and link:
                    items.append({
                        'title': clean_html(title),
                        'url': link,
                        'summary': clean_html(summary)[:300],
                        'date': parse_date(published),
                        'source': source,
                        'source_type': 'news',
                        'state': default_state
                    })
                        
    except ET.ParseError:
        print(f"    ⚠ XML parse error: {source}")
    
    return items


def parse_dot_page(html: str, source: str, state: str, page_type: str, base_url: str) -> List[Dict]:
    items = []
    link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>', re.IGNORECASE)
    
    for match in link_pattern.finditer(html):
        url, text = match.groups()
        text = text.strip()
        
        if len(text) < 10 or len(text) > 250:
            continue
        if any(skip in text.lower() for skip in ['privacy', 'contact us', 'home', 'menu', 'login', 'search', 'skip to', 'accessibility', 'footer']):
            continue
            
        text_lower = text.lower()
        if any(kw in text_lower for kw in ['project', 'bid', 'construction', 'highway', 'bridge', 'contract', 'award', 'route', 'i-', 'us-', 'sr-', 'letting']):
            if url.startswith('/'):
                parsed = urlparse(base_url)
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
            elif not url.startswith('http'):
                continue
            
            items.append({
                'title': text,
                'url': url,
                'summary': f"{state} DOT {page_type} listing",
                'date': datetime.now().strftime('%Y-%m-%d'),
                'source': source,
                'source_type': f'dot_{page_type}',
                'state': state
            })
    
    seen = set()
    unique = []
    for item in items:
        if item['url'] not in seen:
            seen.add(item['url'])
            unique.append(item)
    
    return unique[:15]


def clean_html(text: str) -> str:
    if not text:
        return ''
    clean = re.sub(r'<[^>]+>', '', text)
    clean = re.sub(r'&[a-zA-Z]+;', ' ', clean)
    clean = re.sub(r'&#\d+;', ' ', clean)
    return ' '.join(clean.split())


def parse_date(date_str: str) -> str:
    if not date_str:
        return datetime.now().strftime('%Y-%m-%d')
    
    date_str = re.sub(r'\s*[+-]\d{4}$', '', date_str.strip())
    date_str = re.sub(r'\s*[A-Z]{3,4}$', '', date_str.strip())
    
    formats = [
        '%a, %d %b %Y %H:%M:%S', '%a, %d %b %Y', '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
        '%B %d, %Y', '%b %d, %Y',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str[:30], fmt)
            return dt.strftime('%Y-%m-%d')
        except:
            continue
    
    return datetime.now().strftime('%Y-%m-%d')


def detect_state(text: str, default: str) -> str:
    if default == 'ALL':
        default = 'VT'
        
    text_lower = text.lower()
    scores = {}
    
    for state, patterns in STATE_PATTERNS.items():
        score = sum(1 for p in patterns if p in text_lower)
        if score > 0:
            scores[state] = score
    
    if scores:
        return max(scores, key=scores.get)
    return default


def detect_business_line(text: str) -> str:
    text_lower = text.lower()
    
    if any(kw in text_lower for kw in KEYWORDS_BITUMEN):
        return 'liquid_asphalt'
    if any(kw in text_lower for kw in KEYWORDS_HMA):
        return 'hma'
    if any(kw in text_lower for kw in KEYWORDS_AGGREGATES):
        return 'aggregates'
    if any(kw in text_lower for kw in KEYWORDS_CONCRETE):
        return 'concrete'
    if any(kw in text_lower for kw in KEYWORDS_TRUCKING):
        return 'trucking'
    if any(kw in text_lower for kw in KEYWORDS_HIGHWAY):
        return 'highway'
    
    return 'general'


def score_item(item: Dict) -> Dict:
    text = (item.get('title', '') + ' ' + item.get('summary', '')).lower()
    
    high_matches = sum(1 for kw in ALL_KEYWORDS_HIGH if kw in text)
    med_matches = sum(1 for kw in ALL_KEYWORDS_MEDIUM if kw in text)
    
    score = high_matches * 3 + med_matches
    
    if score >= 5:
        item['priority'] = 'high'
    elif score >= 2:
        item['priority'] = 'medium'
    else:
        item['priority'] = 'low'
    
    if any(kw in text for kw in KEYWORDS_OPPORTUNITY):
        item['category'] = 'bid'
    elif any(kw in text for kw in KEYWORDS_FUNDING):
        item['category'] = 'funding'
    elif any(kw in text for kw in KEYWORDS_HIGHWAY + KEYWORDS_HMA):
        item['category'] = 'dot_project'
    else:
        item['category'] = 'news'
    
    item['business_line'] = detect_business_line(text)
    
    return item


def generate_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


# ============================================================================
# MAIN
# ============================================================================

def run_scraper():
    print("=" * 70)
    print(f"NECMIS Scraper v3.0 - {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)
    print("Coverage: VT, NH, ME, NY, PA, MA, RI, CT")
    print("Business Lines: Highway, HMA, Aggregates, Concrete, Bitumen, Trucking")
    print("=" * 70)
    
    all_items = []
    cutoff = datetime.now() - timedelta(days=DAYS_BACK)
    
    print("\n📰 RSS FEEDS")
    print("-" * 50)
    for name, config in RSS_FEEDS.items():
        state_label = config['state'] if config['state'] != 'ALL' else '🌐'
        print(f"  [{state_label:>2}] {name}...", end=" ", flush=True)
        content = fetch_url(config['url'], name)
        if content:
            items = parse_rss(content, name, config['state'])
            if config['state'] == 'ALL':
                items = [i for i in items if any(
                    kw in (i['title'] + i.get('summary', '')).lower() 
                    for kw in ALL_KEYWORDS_HIGH + ALL_KEYWORDS_MEDIUM
                )]
            print(f"✓ {len(items)} items")
            all_items.extend(items)
        else:
            print("✗")
        time.sleep(0.3)
    
    print("\n🏗️ DOT PAGES")
    print("-" * 50)
    for name, config in DOT_PAGES.items():
        print(f"  [{config['state']:>2}] {name}...", end=" ", flush=True)
        content = fetch_url(config['url'], name)
        if content:
            items = parse_dot_page(content, name, config['state'], config['type'], config['url'])
            print(f"✓ {len(items)} items")
            all_items.extend(items)
        else:
            print("✗")
        time.sleep(0.3)
    
    print("\n⚙️ PROCESSING")
    print("-" * 50)
    
    processed = []
    seen_urls = set()
    
    for item in all_items:
        if item['url'] in seen_urls:
            continue
        seen_urls.add(item['url'])
        
        item['state'] = detect_state(
            item['title'] + ' ' + item.get('summary', ''),
            item.get('state', 'VT')
        )
        
        item = score_item(item)
        item['id'] = generate_id(item['url'])
        
        if item['source_type'] == 'news':
            try:
                item_date = datetime.strptime(item['date'], '%Y-%m-%d')
                if item_date < cutoff:
                    continue
            except:
                pass
        
        processed.append(item)
    
    priority_order = {'high': 0, 'medium': 1, 'low': 2}
    processed.sort(key=lambda x: (priority_order.get(x['priority'], 3), x['date']), reverse=False)
    processed.sort(key=lambda x: priority_order.get(x['priority'], 3))
    
    output = {
        'generated': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'version': '3.0',
        'item_count': len(processed),
        'coverage': {
            'states': STATES,
            'business_lines': ['highway', 'hma', 'aggregates', 'concrete', 'liquid_asphalt', 'trucking']
        },
        'stats': {
            'by_state': {s: len([i for i in processed if i['state'] == s]) for s in STATES},
            'by_priority': {
                'high': len([i for i in processed if i['priority'] == 'high']),
                'medium': len([i for i in processed if i['priority'] == 'medium']),
                'low': len([i for i in processed if i['priority'] == 'low'])
            },
            'by_category': {
                'bid': len([i for i in processed if i['category'] == 'bid']),
                'dot_project': len([i for i in processed if i['category'] == 'dot_project']),
                'funding': len([i for i in processed if i['category'] == 'funding']),
                'news': len([i for i in processed if i['category'] == 'news'])
            },
            'by_business_line': {
                'highway': len([i for i in processed if i.get('business_line') == 'highway']),
                'hma': len([i for i in processed if i.get('business_line') == 'hma']),
                'aggregates': len([i for i in processed if i.get('business_line') == 'aggregates']),
                'concrete': len([i for i in processed if i.get('business_line') == 'concrete']),
                'liquid_asphalt': len([i for i in processed if i.get('business_line') == 'liquid_asphalt']),
                'trucking': len([i for i in processed if i.get('business_line') == 'trucking']),
                'general': len([i for i in processed if i.get('business_line') == 'general'])
            }
        },
        'items': processed
    }
    
    print(f"  Total items: {output['item_count']}")
    print(f"  High priority: {output['stats']['by_priority']['high']}")
    print(f"  Bids: {output['stats']['by_category']['bid']}")
    print(f"  DOT projects: {output['stats']['by_category']['dot_project']}")
    
    print("\n📊 BY STATE")
    print("-" * 50)
    for state in STATES:
        count = output['stats']['by_state'][state]
        bar = '█' * min(count, 20)
        print(f"  {state}: {count:3d} {bar}")
    
    print("\n🏭 BY BUSINESS LINE")
    print("-" * 50)
    for line, count in output['stats']['by_business_line'].items():
        if count > 0:
            print(f"  {line:15s}: {count}")
    
    return output


if __name__ == '__main__':
    data = run_scraper()
    
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'necmis_data.json')
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✅ Output written to {output_path}")
    print("=" * 70)
