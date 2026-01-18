#!/usr/bin/env python3
"""
NECMIS Scraper - Phase 3.0 (Dynamic NHDOT Parser)
==================================================
MA: Plain text parser (PRESERVED - NO CHANGES)
ME: Excel/PDF parser (PRESERVED - NO CHANGES)
NH: Dynamic multi-approach parser (sessions, Playwright, multiple sources)
"""

import json
import hashlib
import re
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    import requests
    import feedparser
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Missing dependency: {e}")
    raise


# =============================================================================
# CONFIGURATION
# =============================================================================

STATES = ['VT', 'NH', 'ME', 'MA', 'NY', 'RI', 'CT', 'PA']

RSS_FEEDS = {
    'VTDigger': {'url': 'https://vtdigger.org/feed/', 'state': 'VT'},
    'Union Leader': {'url': 'https://www.unionleader.com/search/?f=rss&t=article&c=news/business&l=25&s=start_time&sd=desc', 'state': 'NH'},
    'Portland Press Herald': {'url': 'https://www.pressherald.com/feed/', 'state': 'ME'},
    'Bangor Daily News': {'url': 'https://bangordailynews.com/feed/', 'state': 'ME'},
    'InDepthNH': {'url': 'https://indepthnh.org/feed/', 'state': 'NH'},
    'Valley News': {'url': 'https://www.vnews.com/feed/articles/rss', 'state': 'VT'},
    'MassLive': {'url': 'https://www.masslive.com/arc/outboundfeeds/rss/?outputType=xml', 'state': 'MA'},
    'Times Union': {'url': 'https://www.timesunion.com/search/?action=search&channel=news&inlineContent=1&searchindex=solr&query=construction&sort=date&output=rss', 'state': 'NY'},
    'Providence Journal': {'url': 'https://www.providencejournal.com/arcio/rss/', 'state': 'RI'},
    'Hartford Courant': {'url': 'https://www.courant.com/arcio/rss/', 'state': 'CT'},
}

DOT_SOURCES = {
    'MA': {'name': 'MassDOT', 'portal_url': 'https://hwy.massdot.state.ma.us/webapps/const/statusReport.asp', 'parser': 'active'},
    'ME': {'name': 'MaineDOT', 'portal_url': 'https://www.maine.gov/dot/major-projects/cap', 'parser': 'active'},
    'NH': {'name': 'NHDOT', 'portal_url': 'https://www.dot.nh.gov/doing-business-nhdot/contractors/invitation-bid', 'parser': 'active'},
    'VT': {'name': 'VTrans', 'portal_url': 'https://vtrans.vermont.gov/contract-admin/bids-requests/construction-contracting', 'parser': 'stub'},
    'NY': {'name': 'NYSDOT', 'portal_url': 'https://www.dot.ny.gov/doing-business/opportunities/const-highway', 'parser': 'stub'},
    'RI': {'name': 'RIDOT', 'portal_url': 'https://www.dot.ri.gov/about/current_projects.php', 'parser': 'stub'},
    'CT': {'name': 'CTDOT', 'portal_url': 'https://portal.ct.gov/DOT/Doing-Business/Contractor-Information', 'parser': 'stub'},
    'PA': {'name': 'PennDOT', 'portal_url': 'https://www.penndot.pa.gov/business/Letting/Pages/default.aspx', 'parser': 'stub'}
}

# NH Alternative Sources - All verified accessible
NH_LIVE_SOURCES = {
    'stip': [
        # NH STIP PDFs - blocked from GitHub Actions (403), but kept for future reference
        {'name': 'NH STIP Project List', 'url': 'https://mm.nh.gov/files/uploads/dot/remote-docs/2025-2028-stip-project-monthly-list.pdf'},
        {'name': 'NH STIP Current Report', 'url': 'https://mm.nh.gov/files/uploads/dot/remote-docs/stip-current-report-website-0.pdf'},
    ],
    'official': [
        {'name': 'NHDOT ITB', 'url': 'https://www.dot.nh.gov/doing-business-nhdot/contractors/invitation-bid'},
        {'name': 'NHDOT Advertising', 'url': 'https://www.dot.nh.gov/doing-business-nhdot/contractors/advertising-schedule'},
    ],
    'rpc_pdfs': [
        # Direct TIP PDFs from Regional Planning Commissions - THESE WORK from GitHub Actions
        {'name': 'Rockingham TIP Projects', 'url': 'https://www.therpc.org/application/files/2017/3894/2144/Figure12_2025TIPRegional.pdf', 'region': 'Seacoast'},
        {'name': 'Rockingham TIP Full', 'url': 'https://www.therpc.org/download_file/view/3184/481', 'region': 'Seacoast'},
    ],
    'rpc': [
        # HTML pages for fallback/discovery
        {'name': 'Rockingham PC TIP', 'url': 'https://www.therpc.org/transportation/tip/2025-2028-tip', 'region': 'Seacoast'},
        {'name': 'Southern NH PC', 'url': 'https://www.snhpc.org/', 'region': 'Manchester'},
        {'name': 'Nashua RPC', 'url': 'https://www.nashuarpc.org/mpo/transportation_plans.php', 'region': 'Nashua'},
        {'name': 'Central NH RPC', 'url': 'https://cnhrpc.org/transportation-planning/', 'region': 'Concord'},
    ],
    'municipal': [
        {'name': 'Nashua Bids', 'url': 'https://www.nashuanh.gov/Bids.aspx'},
        {'name': 'Manchester Bids', 'url': 'https://www.manchesternh.gov/Departments/Purchasing/Bid-Opportunities-and-Results'},
        {'name': 'Concord Bids', 'url': 'https://www.concordnh.gov/Bids.aspx'},
    ]
}

CONSTRUCTION_KEYWORDS = {
    'high_priority': ['highway', 'bridge', 'DOT', 'bid', 'letting', 'RFP', 'contract award', 'paving', 'resurfacing', 'infrastructure', 'IIJA', 'federal grant'],
    'medium_priority': ['construction', 'road', 'pavement', 'asphalt', 'concrete', 'aggregate', 'gravel', 'development', 'permit', 'municipal'],
    'business_line_keywords': {
        'highway': ['highway', 'road', 'interstate', 'route', 'bridge', 'DOT', 'transportation', 'reconstruction', 'resurfacing'],
        'hma': ['asphalt', 'paving', 'resurfacing', 'overlay', 'milling', 'HMA', 'hot mix', 'surfacing', 'pavement'],
        'aggregates': ['aggregate', 'gravel', 'sand', 'stone', 'quarry'],
        'ready_mix': ['concrete', 'ready-mix', 'cement', 'bridge deck', 'deck'],
        'liquid_asphalt': ['liquid asphalt', 'bitumen', 'emulsion']
    }
}


# =============================================================================
# HELPERS
# =============================================================================

def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:12]

def get_priority(text: str) -> str:
    text_lower = text.lower()
    if any(kw.lower() in text_lower for kw in CONSTRUCTION_KEYWORDS['high_priority']):
        return 'high'
    if any(kw.lower() in text_lower for kw in CONSTRUCTION_KEYWORDS['medium_priority']):
        return 'medium'
    return 'low'

def get_business_lines(text: str) -> List[str]:
    text_lower = text.lower()
    lines = []
    for line, keywords in CONSTRUCTION_KEYWORDS['business_line_keywords'].items():
        if any(kw.lower() in text_lower for kw in keywords):
            lines.append(line)
    return lines if lines else ['highway']

def is_construction_relevant(text: str) -> bool:
    text_lower = text.lower()
    all_kw = CONSTRUCTION_KEYWORDS['high_priority'] + CONSTRUCTION_KEYWORDS['medium_priority']
    return any(kw.lower() in text_lower for kw in all_kw)

def format_currency(amount) -> Optional[str]:
    if amount is None:
        return None
    if amount >= 1000000000:
        return f"${amount / 1000000000:.1f}B"
    elif amount >= 1000000:
        return f"${amount / 1000000:.1f}M"
    elif amount >= 1000:
        return f"${amount / 1000:.0f}K"
    return f"${amount:,.0f}"

def parse_currency(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r'[,$]', '', text.strip())
    try:
        return float(cleaned)
    except ValueError:
        return None

def clean_location(loc: str) -> str:
    if not loc:
        return None
    loc = loc.strip()
    if loc.upper().startswith('DISTRICT'):
        num = re.search(r'\d+', loc)
        return f"District {num.group()}" if num else "Various Locations"
    return loc.title()


# =============================================================================
# BROWSER MIMICKING UTILITIES
# =============================================================================

def get_full_browser_headers():
    """Return headers that fully mimic a real Chrome browser."""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }


def create_browser_session():
    """Create a requests session that mimics a real browser with cookies."""
    session = requests.Session()
    session.headers.update(get_full_browser_headers())
    return session


def fetch_with_session(url: str, session: requests.Session = None, warmup_url: str = None) -> Optional[str]:
    """
    Fetch URL using session with optional warmup to establish cookies.
    Returns HTML content or None if failed.
    """
    if session is None:
        session = create_browser_session()
    
    try:
        # Warmup: hit main domain first to get cookies
        if warmup_url:
            try:
                session.get(warmup_url, timeout=10)
            except:
                pass
        
        response = session.get(url, timeout=30)
        if response.status_code == 200:
            return response.text
        else:
            return None
    except Exception as e:
        print(f"      Session fetch error: {e}")
        return None


def fetch_with_playwright(url: str, wait_for: str = None) -> Optional[str]:
    """
    Fetch URL using Playwright headless browser for JS-rendered content.
    Returns HTML content or None if Playwright unavailable or failed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("      Playwright not installed - skipping JS rendering")
        return None
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            
            # Navigate and wait for content
            page.goto(url, wait_until='networkidle', timeout=30000)
            
            if wait_for:
                try:
                    page.wait_for_selector(wait_for, timeout=10000)
                except:
                    pass
            
            content = page.content()
            browser.close()
            return content
    except Exception as e:
        print(f"      Playwright error: {e}")
        return None


# =============================================================================
# MASSDOT PARSER (Plain Text) - PRESERVED WORKING CODE - NO CHANGES
# =============================================================================

def parse_massdot() -> List[Dict]:
    """Parse MassDOT by converting HTML to plain text first."""
    url = DOT_SOURCES['MA']['portal_url']
    lettings = []
    
    try:
        print(f"    🔍 Fetching MassDOT...")
        response = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        html = response.text
        
        print(f"    📄 Got {len(html)} bytes")
        
        soup = BeautifulSoup(html, 'html.parser')
        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text(separator='\n')
        text = re.sub(r'\n\s*\n', '\n', text)
        
        print(f"    📝 Converted to {len(text)} chars of text")
        
        blocks = re.split(r'(?=Location:)', text)
        print(f"    📦 Found {len(blocks)} potential project blocks")
        
        projects = []
        for block in blocks:
            if 'Project Value:' not in block:
                continue
            
            loc_match = re.search(r'Location:\s*([A-Z][A-Za-z0-9\s\-,]+?)(?:\s+Description:|$)', block)
            desc_match = re.search(r'Description:\s*(.+?)(?:\s+District:|$)', block, re.DOTALL)
            value_match = re.search(r'Project Value:\s*\$([0-9,]+\.?\d*)', block)
            proj_num_match = re.search(r'Project Number:\s*(\d+)', block)
            proj_type_match = re.search(r'Project Type:\s*([^\n]+)', block)
            ad_date_match = re.search(r'Ad Date:\s*(\d{1,2}/\d{1,2}/\d{4})', block)
            district_match = re.search(r'District:\s*(\d+)', block)
            
            if value_match:
                projects.append({
                    'location': loc_match.group(1).strip() if loc_match else None,
                    'description': desc_match.group(1).strip()[:200] if desc_match else None,
                    'value': value_match.group(1),
                    'project_num': proj_num_match.group(1) if proj_num_match else None,
                    'project_type': proj_type_match.group(1).strip() if proj_type_match else None,
                    'ad_date': ad_date_match.group(1) if ad_date_match else None,
                    'district': district_match.group(1) if district_match else None
                })
        
        print(f"    📊 Extracted {len(projects)} projects with values")
        
        if not projects:
            print(f"    🔄 Trying line-by-line extraction...")
            values = re.findall(r'Project Value:\s*\$([0-9,]+\.?\d*)', text)
            locations = re.findall(r'Location:\s*([A-Z][A-Za-z0-9\s\-,]+)', text)
            descriptions = re.findall(r'Description:\s*(.+?)(?=\s*District:|\n)', text)
            proj_nums = re.findall(r'Project Number:\s*(\d+)', text)
            proj_types = re.findall(r'Project Type:\s*([^\n]+)', text)
            ad_dates = re.findall(r'Ad Date:\s*(\d{1,2}/\d{1,2}/\d{4})', text)
            districts = re.findall(r'District:\s*(\d+)\s*Ad Date:', text)
            
            print(f"    Line extraction: {len(values)} val, {len(locations)} loc")
            
            for i in range(len(values)):
                projects.append({
                    'location': locations[i] if i < len(locations) else None,
                    'description': descriptions[i][:200] if i < len(descriptions) else None,
                    'value': values[i],
                    'project_num': proj_nums[i] if i < len(proj_nums) else None,
                    'project_type': proj_types[i].strip() if i < len(proj_types) else None,
                    'ad_date': ad_dates[i] if i < len(ad_dates) else None,
                    'district': districts[i] if i < len(districts) else None
                })
        
        if not projects:
            print(f"    🔄 Falling back to dollar-only extraction...")
            all_values = re.findall(r'\$([0-9,]+\.?\d*)', text)
            for i, v in enumerate(all_values):
                val = parse_currency(v)
                if val and 100000 <= val <= 500000000:
                    projects.append({
                        'location': None, 'description': f"MassDOT Project #{i+1}",
                        'value': v, 'project_num': None, 'project_type': None,
                        'ad_date': None, 'district': None
                    })
        
        for p in projects[:50]:
            cost = parse_currency(p['value'])
            if not cost:
                continue
            
            location = clean_location(p['location'])
            desc = p['description'] or f"MassDOT Project - {location or 'Various Locations'}"
            desc = re.sub(r'\s+', ' ', desc).strip()
            
            proj_type = p['project_type']
            if proj_type:
                proj_type = re.sub(r'\s*,\s*$', '', proj_type)[:60]
            
            ad_date = None
            if p['ad_date']:
                try:
                    ad_date = datetime.strptime(p['ad_date'], '%m/%d/%Y').strftime('%Y-%m-%d')
                except:
                    pass
            
            district = int(p['district']) if p['district'] else None
            project_url = f"{url}?projnum={p['project_num']}" if p['project_num'] else url
            
            lettings.append({
                'id': generate_id(f"MA-{p['project_num'] or cost}-{desc[:25]}"),
                'state': 'MA',
                'project_id': p['project_num'],
                'description': desc[:200],
                'cost_low': int(cost),
                'cost_high': int(cost),
                'cost_display': format_currency(cost),
                'ad_date': ad_date,
                'let_date': None,
                'project_type': proj_type,
                'location': location,
                'district': district,
                'url': project_url,
                'source': 'MassDOT',
                'business_lines': get_business_lines(f"{desc} {proj_type or ''}")
            })
        
        if lettings:
            total = sum(l.get('cost_low') or 0 for l in lettings)
            print(f"    ✓ {len(lettings)} projects, {format_currency(total)} total pipeline")
        else:
            print(f"    ⚠ No projects parsed")
            lettings.append(create_portal_stub('MA'))
            
    except Exception as e:
        print(f"    ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        lettings.append(create_portal_stub('MA'))
    
    return lettings


# =============================================================================
# MAINEDOT PARSER - PRESERVED WORKING CODE (Excel + PDF) - NO CHANGES
# =============================================================================

def parse_mainedot() -> List[Dict]:
    """Parse MaineDOT CAP - Excel primary, PDF backup."""
    lettings = []
    cap_url = "https://www.maine.gov/dot/major-projects/cap"
    excel_url = "https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.xls"
    pdf_url = "https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.pdf"
    
    # === ATTEMPT 1: Excel file ===
    try:
        print(f"    🔍 Fetching MaineDOT CAP Excel...")
        response = requests.get(excel_url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        print(f"    📊 Got Excel: {len(response.content)} bytes")
        
        try:
            import pandas as pd
            import io
            
            df = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
            print(f"    📋 Excel has {len(df)} rows")
            
            col_map = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'work' in col_lower and 'type' in col_lower:
                    col_map['work_type'] = col
                elif 'advertise' in col_lower and 'date' in col_lower:
                    col_map['ad_date'] = col
                elif 'location' in col_lower or 'title' in col_lower:
                    col_map['location'] = col
                elif 'detail' in col_lower or 'description' in col_lower:
                    col_map['details'] = col
                elif 'project' in col_lower and ('id' in col_lower or 'no' in col_lower or 'identification' in col_lower):
                    col_map['project_id'] = col
                elif 'estimate' in col_lower or 'cost' in col_lower or 'total' in col_lower:
                    col_map['cost'] = col
            
            for idx, row in df.iterrows():
                try:
                    project_id = str(row[col_map['project_id']]) if 'project_id' in col_map and pd.notna(row[col_map['project_id']]) else None
                    if not project_id or project_id == 'nan':
                        continue
                    
                    work_type = str(row[col_map['work_type']]) if 'work_type' in col_map and pd.notna(row[col_map['work_type']]) else None
                    location = str(row[col_map['location']]) if 'location' in col_map and pd.notna(row[col_map['location']]) else None
                    details = str(row[col_map['details']]) if 'details' in col_map and pd.notna(row[col_map['details']]) else None
                    
                    cost = None
                    if 'cost' in col_map and pd.notna(row[col_map['cost']]):
                        cost_val = row[col_map['cost']]
                        if isinstance(cost_val, (int, float)):
                            cost = int(cost_val)
                        else:
                            cost = parse_currency(str(cost_val))
                            if cost:
                                cost = int(cost)
                    
                    ad_date = None
                    if 'ad_date' in col_map and pd.notna(row[col_map['ad_date']]):
                        date_val = row[col_map['ad_date']]
                        if isinstance(date_val, datetime):
                            ad_date = date_val.strftime('%Y-%m-%d')
                        else:
                            try:
                                ad_date = datetime.strptime(str(date_val), '%m/%d/%Y').strftime('%Y-%m-%d')
                            except:
                                pass
                    
                    proj_type = None
                    if work_type:
                        work_lower = work_type.lower()
                        if 'bridge' in work_lower:
                            proj_type = 'Bridge'
                        elif 'paving' in work_lower or 'preservation' in work_lower:
                            proj_type = 'Pavement'
                        elif 'highway' in work_lower:
                            proj_type = 'Highway'
                        elif 'safety' in work_lower:
                            proj_type = 'Safety'
                        else:
                            proj_type = work_type[:30]
                    
                    description = location or details or f"MaineDOT Project {project_id}"
                    if details and location:
                        description = f"{location}: {details}"
                    
                    lettings.append({
                        'id': generate_id(f"ME-{project_id}-{description[:20]}"),
                        'state': 'ME',
                        'project_id': project_id,
                        'description': description[:200],
                        'cost_low': cost,
                        'cost_high': cost,
                        'cost_display': format_currency(cost) if cost else 'TBD',
                        'ad_date': ad_date,
                        'let_date': ad_date,
                        'project_type': proj_type or work_type,
                        'location': location.split(',')[0] if location and ',' in location else location,
                        'district': None,
                        'url': cap_url,
                        'source': 'MaineDOT CAP',
                        'business_lines': get_business_lines(f"{work_type} {location} {details}")
                    })
                except:
                    continue
            
            if lettings:
                seen_ids = set()
                unique = []
                for l in lettings:
                    key = l['project_id'] or l['description'][:50]
                    if key not in seen_ids:
                        seen_ids.add(key)
                        unique.append(l)
                lettings = unique
                
                total = sum(l.get('cost_low') or 0 for l in lettings)
                with_cost = len([l for l in lettings if l.get('cost_low')])
                print(f"    ✓ {len(lettings)} projects from Excel ({with_cost} with $), {format_currency(total)} pipeline")
                return lettings
                
        except ImportError as e:
            print(f"    ⚠ pandas/xlrd not installed: {e}")
        except Exception as e:
            print(f"    ⚠ Excel parse error: {e}")
    except Exception as e:
        print(f"    ⚠ Excel fetch failed: {e}")
    
    # === ATTEMPT 2: PDF ===
    try:
        print(f"    🔄 Fetching MaineDOT CAP PDF...")
        response = requests.get(pdf_url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        print(f"    📄 Got PDF: {len(response.content)} bytes")
        
        try:
            import pdfplumber
            import io
            
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                print(f"    📑 PDF has {len(pdf.pages)} pages")
                
                current_work_type = None
                work_type_headers = [
                    'Bridge Construction', 'Bridges Other', 'Highway Construction', 
                    'Highway Preservation Paving', 'Highway Rehabilitation',
                    'Highway Safety and Spot Improvements', 'Multimodal', 'Maintenance',
                    'Highway Light Capital Paving'
                ]
                
                for page in pdf.pages:
                    text = page.extract_text() or ''
                    for line in text.split('\n'):
                        line_stripped = line.strip()
                        
                        if line_stripped in work_type_headers:
                            current_work_type = line_stripped
                            continue
                        
                        if not line_stripped or 'Plan Advertise Date' in line:
                            continue
                        
                        id_match = re.search(r'(\d{6}\.\d{2})', line)
                        cost_match = re.search(r'\$([\d,]+)', line)
                        
                        if id_match and cost_match and current_work_type:
                            project_id = id_match.group(1)
                            try:
                                cost = int(cost_match.group(1).replace(',', ''))
                            except:
                                cost = None
                            
                            date_match = re.search(r'^(\d{2}/\d{2}/\d{4})', line)
                            let_date = None
                            if date_match:
                                try:
                                    let_date = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                                except:
                                    pass
                            
                            location = line
                            if date_match:
                                location = location[len(date_match.group(0)):].strip()
                            location = re.sub(r'\d{6}\.\d{2}', '', location)
                            location = re.sub(r'\$[\d,]+', '', location).strip()
                            
                            proj_type = None
                            if 'bridge' in current_work_type.lower():
                                proj_type = 'Bridge'
                            elif 'paving' in current_work_type.lower():
                                proj_type = 'Pavement'
                            elif 'highway' in current_work_type.lower():
                                proj_type = 'Highway'
                            elif 'safety' in current_work_type.lower():
                                proj_type = 'Safety'
                            
                            if location and len(location) > 3:
                                lettings.append({
                                    'id': generate_id(f"ME-{project_id}-{location[:20]}"),
                                    'state': 'ME',
                                    'project_id': project_id,
                                    'description': location[:200],
                                    'cost_low': cost,
                                    'cost_high': cost,
                                    'cost_display': format_currency(cost) if cost else 'TBD',
                                    'ad_date': let_date,
                                    'let_date': let_date,
                                    'project_type': proj_type or current_work_type,
                                    'location': location.split(',')[0] if ',' in location else location,
                                    'district': None,
                                    'url': cap_url,
                                    'source': 'MaineDOT CAP',
                                    'business_lines': get_business_lines(f"{current_work_type} {location}")
                                })
                
                if lettings:
                    seen_ids = set()
                    unique = []
                    for l in lettings:
                        if l['project_id'] not in seen_ids:
                            seen_ids.add(l['project_id'])
                            unique.append(l)
                    lettings = unique
                    
                    total = sum(l.get('cost_low') or 0 for l in lettings)
                    with_cost = len([l for l in lettings if l.get('cost_low')])
                    print(f"    ✓ {len(lettings)} projects from PDF ({with_cost} with $), {format_currency(total)} pipeline")
                    return lettings
                    
        except ImportError:
            print(f"    ⚠ pdfplumber not installed - trying PyPDF2...")
            try:
                import PyPDF2
                import io
                
                reader = PyPDF2.PdfReader(io.BytesIO(response.content))
                print(f"    📑 PDF has {len(reader.pages)} pages (PyPDF2)")
                
                current_work_type = None
                work_type_headers = [
                    'Bridge Construction', 'Bridges Other', 'Highway Construction', 
                    'Highway Preservation Paving', 'Highway Rehabilitation',
                    'Highway Safety and Spot Improvements', 'Multimodal', 'Maintenance',
                    'Highway Light Capital Paving'
                ]
                
                for page in reader.pages:
                    text = page.extract_text() or ''
                    for line in text.split('\n'):
                        line_stripped = line.strip()
                        
                        if line_stripped in work_type_headers:
                            current_work_type = line_stripped
                            continue
                        
                        if not line_stripped or 'Plan Advertise Date' in line:
                            continue
                        
                        id_match = re.search(r'(\d{6}\.\d{2})', line)
                        cost_match = re.search(r'\$([\d,]+)', line)
                        
                        if id_match and cost_match and current_work_type:
                            project_id = id_match.group(1)
                            try:
                                cost = int(cost_match.group(1).replace(',', ''))
                            except:
                                cost = None
                            
                            date_match = re.search(r'^(\d{2}/\d{2}/\d{4})', line)
                            let_date = None
                            if date_match:
                                try:
                                    let_date = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                                except:
                                    pass
                            
                            location = line
                            if date_match:
                                location = location[len(date_match.group(0)):].strip()
                            location = re.sub(r'\d{6}\.\d{2}', '', location)
                            location = re.sub(r'\$[\d,]+', '', location).strip()
                            
                            proj_type = None
                            if 'bridge' in current_work_type.lower():
                                proj_type = 'Bridge'
                            elif 'paving' in current_work_type.lower():
                                proj_type = 'Pavement'
                            elif 'highway' in current_work_type.lower():
                                proj_type = 'Highway'
                            
                            if location and len(location) > 3:
                                lettings.append({
                                    'id': generate_id(f"ME-{project_id}-{location[:20]}"),
                                    'state': 'ME',
                                    'project_id': project_id,
                                    'description': location[:200],
                                    'cost_low': cost,
                                    'cost_high': cost,
                                    'cost_display': format_currency(cost) if cost else 'TBD',
                                    'ad_date': let_date,
                                    'let_date': let_date,
                                    'project_type': proj_type or current_work_type,
                                    'location': location.split(',')[0] if ',' in location else location,
                                    'district': None,
                                    'url': cap_url,
                                    'source': 'MaineDOT CAP',
                                    'business_lines': get_business_lines(f"{current_work_type} {location}")
                                })
                
                if lettings:
                    seen_ids = set()
                    unique = []
                    for l in lettings:
                        if l['project_id'] not in seen_ids:
                            seen_ids.add(l['project_id'])
                            unique.append(l)
                    lettings = unique
                    
                    total = sum(l.get('cost_low') or 0 for l in lettings)
                    print(f"    ✓ {len(lettings)} projects from PDF/PyPDF2, {format_currency(total)} pipeline")
                    return lettings
            except ImportError:
                print(f"    ⚠ PyPDF2 not installed")
            except Exception as e:
                print(f"    ⚠ PyPDF2 error: {e}")
        except Exception as e:
            print(f"    ⚠ PDF parse error: {e}")
    except Exception as e:
        print(f"    ⚠ PDF fetch failed: {e}")
    
    print(f"    ⚠ All sources failed, using portal stub")
    return [create_portal_stub('ME')]


# =============================================================================
# NHDOT PARSER - DYNAMIC MULTI-APPROACH (NEW IMPLEMENTATION)
# =============================================================================

def parse_nhdot() -> List[Dict]:
    """
    Parse NHDOT using dynamic multi-approach strategy:
    
    Tier 0: NH STIP PDF (authoritative statewide project list with costs) - PRIMARY
    Tier 1: Official NHDOT with session + full browser headers + cookies
    Tier 2: Playwright headless browser for JS-rendered content  
    Tier 3: Regional Planning Commission TIPs (live alternatives)
    Tier 4: Municipal bid pages
    
    NO STATIC FALLBACK - returns real data or portal stub with clear message
    """
    lettings = []
    sources_tried = []
    itb_url = DOT_SOURCES['NH']['portal_url']
    
    print(f"    📋 NHDOT Dynamic Multi-Approach Parser")
    
    # ==========================================================================
    # TIER 0: NH STIP PDF (Primary Source - Authoritative Project List)
    # ==========================================================================
    print(f"    🔍 Tier 0: NH STIP PDF (Primary)...")
    
    for stip_source in NH_LIVE_SOURCES.get('stip', []):
        try:
            response = requests.get(stip_source['url'], timeout=60, headers=get_full_browser_headers())
            
            if response.status_code != 200:
                sources_tried.append(f"{stip_source['name']}: {response.status_code}")
                continue
            
            sources_tried.append(f"{stip_source['name']}: {len(response.content)} bytes")
            
            # Parse STIP PDF
            parsed = parse_nh_stip_pdf(response.content, stip_source['url'])
            if parsed:
                lettings.extend(parsed)
                
        except Exception as e:
            sources_tried.append(f"{stip_source['name']}: {type(e).__name__}")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 0 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # TIER 1: Official NHDOT with Session + Full Browser Mimicking
    # ==========================================================================
    print(f"    🔍 Tier 1: Session + Full Browser Headers...")
    
    session = create_browser_session()
    
    # Warmup: Hit main NH.gov domain first to get cookies
    try:
        session.get('https://www.nh.gov/', timeout=10)
        session.get('https://www.dot.nh.gov/', timeout=10)
    except:
        pass
    
    # Try the official ITB page
    for source in NH_LIVE_SOURCES.get('official', []):
        try:
            response = session.get(source['url'], timeout=30)
            
            if response.status_code == 403:
                sources_tried.append(f"{source['name']}: 403 (session)")
                continue
            elif response.status_code != 200:
                sources_tried.append(f"{source['name']}: {response.status_code}")
                continue
            
            html = response.text
            sources_tried.append(f"{source['name']}: {len(html)} bytes")
            
            # Parse the HTML for project data
            parsed = parse_nhdot_html(html, source['url'], source['name'])
            if parsed:
                lettings.extend(parsed)
                
        except Exception as e:
            sources_tried.append(f"{source['name']}: {type(e).__name__}")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 1 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # TIER 2: Playwright Headless Browser (for JS-rendered content)
    # ==========================================================================
    print(f"    🔍 Tier 2: Playwright Headless Browser...")
    
    for source in NH_LIVE_SOURCES.get('official', []):
        html = fetch_with_playwright(source['url'], wait_for='table')
        
        if html:
            sources_tried.append(f"{source['name']}: Playwright {len(html)} bytes")
            parsed = parse_nhdot_html(html, source['url'], source['name'])
            if parsed:
                lettings.extend(parsed)
        else:
            sources_tried.append(f"{source['name']}: Playwright failed")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 2 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # TIER 3: RPC TIP PDFs (Direct Links - Best Source for Costs)
    # ==========================================================================
    print(f"    🔍 Tier 3: RPC TIP PDFs (Direct Links)...")
    
    for rpc_pdf in NH_LIVE_SOURCES.get('rpc_pdfs', []):
        try:
            response = session.get(rpc_pdf['url'], timeout=60, allow_redirects=True)
            
            if response.status_code != 200:
                sources_tried.append(f"{rpc_pdf['name']}: {response.status_code}")
                continue
            
            # Parse TIP PDF using dedicated parser
            parsed = parse_rpc_tip_pdf_detailed(response.content, rpc_pdf['name'], rpc_pdf['region'], rpc_pdf['url'])
            if parsed:
                lettings.extend(parsed)
                sources_tried.append(f"{rpc_pdf['name']}: PDF {len(parsed)} projects")
            else:
                sources_tried.append(f"{rpc_pdf['name']}: PDF parse failed")
                
        except Exception as e:
            sources_tried.append(f"{rpc_pdf['name']}: {type(e).__name__}")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 3 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # TIER 4: Regional Planning Commission HTML Pages (Fallback)
    # ==========================================================================
    print(f"    🔍 Tier 4: RPC HTML Pages...")
    
    for rpc in NH_LIVE_SOURCES.get('rpc', []):
        try:
            # Use session for RPC sites too
            response = session.get(rpc['url'], timeout=30)
            
            if response.status_code != 200:
                sources_tried.append(f"{rpc['name']}: {response.status_code}")
                continue
            
            content_type = response.headers.get('content-type', '')
            
            # Handle PDF TIPs
            if 'pdf' in content_type or rpc['url'].endswith('.pdf'):
                parsed = parse_rpc_tip_pdf(response.content, rpc['name'], rpc['region'])
                if parsed:
                    lettings.extend(parsed)
                    sources_tried.append(f"{rpc['name']}: PDF {len(parsed)} projects")
                else:
                    sources_tried.append(f"{rpc['name']}: PDF no projects")
            else:
                # Parse HTML
                parsed = parse_rpc_html(response.text, rpc['url'], rpc['name'], rpc['region'])
                if parsed:
                    lettings.extend(parsed)
                    sources_tried.append(f"{rpc['name']}: HTML {len(parsed)} projects")
                else:
                    sources_tried.append(f"{rpc['name']}: HTML no projects")
                    
        except Exception as e:
            sources_tried.append(f"{rpc['name']}: {type(e).__name__}")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 4 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # TIER 5: Municipal Bid Pages
    # ==========================================================================
    print(f"    🔍 Tier 5: Municipal Bid Pages...")
    
    for muni in NH_LIVE_SOURCES.get('municipal', []):
        try:
            response = session.get(muni['url'], timeout=30)
            
            if response.status_code != 200:
                sources_tried.append(f"{muni['name']}: {response.status_code}")
                continue
            
            parsed = parse_municipal_bids(response.text, muni['url'], muni['name'])
            if parsed:
                lettings.extend(parsed)
                sources_tried.append(f"{muni['name']}: {len(parsed)} bids")
            else:
                sources_tried.append(f"{muni['name']}: no bids")
                
        except Exception as e:
            sources_tried.append(f"{muni['name']}: {type(e).__name__}")
    
    if lettings:
        total = sum(l.get('cost_low') or 0 for l in lettings)
        print(f"    ✓ Tier 4 success: {len(lettings)} projects, {format_currency(total)}")
        print(f"      Sources: {', '.join(sources_tried)}")
        return lettings
    
    # ==========================================================================
    # ALL TIERS FAILED - NO STATIC FALLBACK
    # ==========================================================================
    print(f"    ⚠ All dynamic sources failed")
    print(f"      Tried: {', '.join(sources_tried)}")
    
    # Return portal stub with clear message - NO STATIC DATA
    return [create_portal_stub('NH')]


def parse_nh_stip_pdf(pdf_content: bytes, url: str) -> List[Dict]:
    """
    Parse NH STIP Monthly Project List PDF.
    
    Format:
    - Project ID: 5-digit number like 42437, 44160
    - Location: TOWN-TOWN format like BETHLEHEM-LITTLETON
    - Route: I-93, NH 18, US 3, etc.
    - Cost: $24,652,457 format
    - RPC region
    - Phase info (PE, ROW, CON)
    """
    lettings = []
    
    try:
        import pdfplumber
        import io
        
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            print(f"      STIP PDF has {len(pdf.pages)} pages")
            
            seen_projects = set()
            
            for page in pdf.pages:
                text = page.extract_text() or ''
                
                # Split into lines and process
                lines = text.split('\n')
                
                for i, line in enumerate(lines):
                    # Skip headers and empty lines
                    if not line.strip() or 'Report Project List' in line or 'Page' in line:
                        continue
                    
                    # Look for project ID pattern: (5-digit number)
                    # Format: "LOCATION (PROJECT_ID) ROUTE"
                    project_match = re.search(r'\((\d{5})\)', line)
                    if not project_match:
                        continue
                    
                    project_id = project_match.group(1)
                    
                    # Skip if we've already seen this project
                    if project_id in seen_projects:
                        continue
                    seen_projects.add(project_id)
                    
                    # Extract location (text before the project ID)
                    location_part = line[:project_match.start()].strip()
                    # Clean up location - remove any leading numbers/dates
                    location = re.sub(r'^\d+[\s/]*\d*[\s/]*\d*\s*', '', location_part).strip()
                    
                    # Extract route (text after project ID)
                    route_part = line[project_match.end():].strip()
                    route_match = re.search(r'(I-\d+|US\s*\d+|NH\s*\d+|SR\s*\d+)', route_part, re.I)
                    route = route_match.group(1) if route_match else None
                    
                    # Look for cost in this line or nearby lines
                    cost = None
                    # Check current line and next few lines for cost
                    search_text = ' '.join(lines[i:min(i+5, len(lines))])
                    
                    # Look for "Project Cost: $X" or "All Project Cost: $X"
                    cost_match = re.search(r'(?:All\s+)?Project\s+Cost:\s*\$([\d,]+)', search_text, re.I)
                    if cost_match:
                        cost = parse_currency(cost_match.group(1))
                    else:
                        # Look for standalone dollar amounts in reasonable range
                        dollar_matches = re.findall(r'\$([\d,]+(?:\.\d{2})?)', search_text)
                        for dm in dollar_matches:
                            val = parse_currency(dm)
                            if val and 100000 <= val <= 1000000000:  # $100K to $1B
                                cost = val
                                break
                    
                    # Determine project type from route/location
                    combined_text = f"{location} {route or ''}"
                    proj_type = classify_project_type(combined_text)
                    
                    # Build description
                    description = location
                    if route:
                        description = f"{location} - {route}"
                    
                    # Extract RPC region if present
                    rpc_match = re.search(r'(NCC|RPC|SNHPC|NRPC|CNHRPC|SRPC|SWRPC|LRPC|UVLSRPC)', search_text)
                    district = rpc_match.group(1) if rpc_match else None
                    
                    lettings.append({
                        'id': generate_id(f"NH-STIP-{project_id}"),
                        'state': 'NH',
                        'project_id': project_id,
                        'description': description[:200],
                        'cost_low': int(cost) if cost else None,
                        'cost_high': int(cost) if cost else None,
                        'cost_display': format_currency(cost) if cost else 'See STIP',
                        'ad_date': None,
                        'let_date': None,
                        'project_type': proj_type,
                        'location': location.split('-')[0] if '-' in location else location,
                        'district': district,
                        'url': url,
                        'source': 'NH STIP',
                        'business_lines': get_business_lines(combined_text)
                    })
            
            if lettings:
                # Sort by cost (highest first) for better visibility
                lettings.sort(key=lambda x: x.get('cost_low') or 0, reverse=True)
                
                total = sum(l.get('cost_low') or 0 for l in lettings)
                with_cost = len([l for l in lettings if l.get('cost_low')])
                print(f"      Parsed {len(lettings)} projects ({with_cost} with $), {format_currency(total)} total")
                return lettings
                
    except ImportError:
        print("      pdfplumber not installed - cannot parse STIP PDF")
    except Exception as e:
        print(f"      STIP PDF parse error: {e}")
        import traceback
        traceback.print_exc()
    
    return []


def parse_nhdot_html(html: str, url: str, source_name: str) -> List[Dict]:
    """Parse NHDOT HTML page for project data."""
    lettings = []
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for tables with bid/project data
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        headers = []
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            
            # Detect header row
            if row.find_all('th'):
                headers = [c.get_text(strip=True).lower() for c in cells]
                continue
            
            if not headers or len(cells) < 3:
                continue
            
            # Try to extract project data
            row_data = {headers[i] if i < len(headers) else f'col{i}': cells[i].get_text(strip=True) 
                       for i in range(len(cells))}
            
            # Look for project number patterns
            project_id = None
            for key, val in row_data.items():
                if re.match(r'^\d{5}[A-Z]?$', val):
                    project_id = val
                    break
                match = re.search(r'(\d{5}[A-Z]?)', val)
                if match:
                    project_id = match.group(1)
                    break
            
            if not project_id:
                continue
            
            # Extract cost
            cost = None
            for key, val in row_data.items():
                if 'estimate' in key or 'cost' in key or 'amount' in key:
                    cost = parse_currency(val)
                    break
                cost_match = re.search(r'\$[\d,]+', val)
                if cost_match:
                    cost = parse_currency(cost_match.group())
            
            # Extract description/location
            description = None
            location = None
            for key, val in row_data.items():
                if 'description' in key or 'project' in key or 'title' in key:
                    description = val[:200]
                if 'location' in key or 'town' in key or 'city' in key:
                    location = val
            
            if not description:
                description = ' '.join(row_data.values())[:200]
            
            lettings.append({
                'id': generate_id(f"NH-{project_id}-{description[:20]}"),
                'state': 'NH',
                'project_id': project_id,
                'description': description,
                'cost_low': int(cost) if cost else None,
                'cost_high': int(cost) if cost else None,
                'cost_display': format_currency(cost) if cost else 'See Bid Docs',
                'ad_date': None,
                'let_date': None,
                'project_type': classify_project_type(description),
                'location': location,
                'district': None,
                'url': url,
                'source': source_name,
                'business_lines': get_business_lines(description)
            })
    
    # Also try to find project info in divs/sections
    if not lettings:
        # Look for bid items in common HTML patterns
        bid_items = soup.find_all(['div', 'section', 'article'], 
                                   class_=lambda x: x and any(k in str(x).lower() for k in ['bid', 'project', 'contract']))
        
        for item in bid_items:
            text = item.get_text()
            
            # Look for project ID pattern
            id_match = re.search(r'(\d{5}[A-Z]?)', text)
            if not id_match:
                continue
            
            project_id = id_match.group(1)
            
            # Look for cost
            cost = None
            cost_match = re.search(r'\$([\d,]+(?:\.\d{2})?)', text)
            if cost_match:
                cost = parse_currency(cost_match.group(1))
            
            # Get description (first ~200 chars)
            description = re.sub(r'\s+', ' ', text)[:200]
            
            lettings.append({
                'id': generate_id(f"NH-{project_id}-{description[:20]}"),
                'state': 'NH',
                'project_id': project_id,
                'description': description,
                'cost_low': int(cost) if cost else None,
                'cost_high': int(cost) if cost else None,
                'cost_display': format_currency(cost) if cost else 'See Bid Docs',
                'ad_date': None,
                'let_date': None,
                'project_type': classify_project_type(description),
                'location': None,
                'district': None,
                'url': url,
                'source': source_name,
                'business_lines': get_business_lines(description)
            })
    
    return lettings


def parse_rpc_tip_pdf(pdf_content: bytes, rpc_name: str, region: str) -> List[Dict]:
    """Parse Regional Planning Commission TIP PDF for project data."""
    lettings = []
    
    try:
        import pdfplumber
        import io
        
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                
                # Look for NHDOT project patterns
                for line in text.split('\n'):
                    # NHDOT project ID pattern
                    id_match = re.search(r'(\d{5}[A-Z]?)', line)
                    if not id_match:
                        continue
                    
                    project_id = id_match.group(1)
                    
                    # Look for cost
                    cost = None
                    cost_match = re.search(r'\$([\d,]+)', line)
                    if cost_match:
                        cost = parse_currency(cost_match.group(1))
                    
                    # Clean up description
                    description = re.sub(r'\d{5}[A-Z]?', '', line)
                    description = re.sub(r'\$[\d,]+', '', description)
                    description = re.sub(r'\s+', ' ', description).strip()[:200]
                    
                    if description and len(description) > 10:
                        lettings.append({
                            'id': generate_id(f"NH-RPC-{project_id}-{description[:20]}"),
                            'state': 'NH',
                            'project_id': project_id,
                            'description': f"{region}: {description}",
                            'cost_low': int(cost) if cost else None,
                            'cost_high': int(cost) if cost else None,
                            'cost_display': format_currency(cost) if cost else 'TBD',
                            'ad_date': None,
                            'let_date': None,
                            'project_type': classify_project_type(description),
                            'location': region,
                            'district': None,
                            'url': f"https://{rpc_name.lower().replace(' ', '')}.org",
                            'source': f'{rpc_name} TIP',
                            'business_lines': get_business_lines(description)
                        })
    except ImportError:
        pass
    except Exception:
        pass
    
    return lettings


def parse_rpc_tip_pdf_detailed(pdf_content: bytes, rpc_name: str, region: str, url: str) -> List[Dict]:
    """
    Parse Rockingham-style RPC TIP PDF with detailed project data.
    
    Format:
    LOCATION (PROJECT_ID)
    Phase 2025 2026 2027 2028 Total
    Facility: ROUTE
    SCOPE: Description
    FEDERAL STATE OTHER
    Total Cost: $XX,XXX,XXX
    """
    lettings = []
    
    try:
        import pdfplumber
        import io
        
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            print(f"      RPC PDF has {len(pdf.pages)} pages")
            
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text() or ''
                full_text += text + "\n"
            
            # Split into project blocks
            # Each project starts with "LOCATION (5-digit-ID)"
            project_pattern = re.compile(r'([A-Z][A-Z\s\-]+?)\s*\((\d{5}[A-Z]?)\)')
            
            # Find all project headers
            matches = list(project_pattern.finditer(full_text))
            
            seen_projects = set()
            
            for i, match in enumerate(matches):
                location = match.group(1).strip()
                project_id = match.group(2)
                
                # Skip duplicates
                if project_id in seen_projects:
                    continue
                seen_projects.add(project_id)
                
                # Get the text block for this project (until next project or end)
                start_pos = match.start()
                if i + 1 < len(matches):
                    end_pos = matches[i + 1].start()
                else:
                    end_pos = len(full_text)
                
                project_text = full_text[start_pos:end_pos]
                
                # Extract Facility/Route
                facility_match = re.search(r'Facility:\s*(.+?)(?:\n|SCOPE)', project_text, re.DOTALL)
                facility = facility_match.group(1).strip() if facility_match else None
                
                # Extract Scope/Description
                scope_match = re.search(r'SCOPE:\s*(.+?)(?:FEDERAL|Total Cost)', project_text, re.DOTALL)
                scope = scope_match.group(1).strip().replace('\n', ' ') if scope_match else None
                
                # Extract Total Cost
                cost = None
                cost_match = re.search(r'Total Cost:\s*\$([\d,]+)', project_text)
                if cost_match:
                    cost = parse_currency(cost_match.group(1))
                else:
                    # Try alternate patterns
                    cost_match = re.search(r'2025-2028 Funding:\s*\$([\d,]+)', project_text)
                    if cost_match:
                        cost = parse_currency(cost_match.group(1))
                
                # Skip very small projects or programs
                if cost and cost < 50000:
                    continue
                
                # Skip transit/program entries
                if 'PROGRAM' in location or 'FTA' in (facility or '') or 'TRANSIT' in location.upper():
                    continue
                
                # Build description
                if facility and scope:
                    description = f"{facility}: {scope}"
                elif scope:
                    description = scope
                elif facility:
                    description = facility
                else:
                    description = location
                
                # Clean description
                description = re.sub(r'\s+', ' ', description).strip()[:200]
                
                # Determine project type
                combined = f"{location} {facility or ''} {scope or ''}"
                proj_type = classify_project_type(combined)
                
                lettings.append({
                    'id': generate_id(f"NH-RPC-{project_id}"),
                    'state': 'NH',
                    'project_id': project_id,
                    'description': f"{location}: {description}",
                    'cost_low': int(cost) if cost else None,
                    'cost_high': int(cost) if cost else None,
                    'cost_display': format_currency(cost) if cost else 'See TIP',
                    'ad_date': None,
                    'let_date': None,
                    'project_type': proj_type,
                    'location': location.split('-')[0].strip() if '-' in location else location.strip(),
                    'district': region,
                    'url': url,
                    'source': f'{rpc_name}',
                    'business_lines': get_business_lines(combined)
                })
            
            if lettings:
                # Sort by cost (highest first)
                lettings.sort(key=lambda x: x.get('cost_low') or 0, reverse=True)
                
                total = sum(l.get('cost_low') or 0 for l in lettings)
                with_cost = len([l for l in lettings if l.get('cost_low')])
                print(f"      Parsed {len(lettings)} projects ({with_cost} with costs), {format_currency(total)} total")
                
    except ImportError:
        print("      pdfplumber not installed")
    except Exception as e:
        print(f"      RPC PDF parse error: {e}")
        import traceback
        traceback.print_exc()
    
    return lettings


def parse_rpc_html(html: str, url: str, rpc_name: str, region: str) -> List[Dict]:
    """Parse Regional Planning Commission HTML page for TIP project data."""
    lettings = []
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for links to TIP documents or project listings
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        
        # Look for TIP PDF links
        if '.pdf' in href.lower() and any(k in text.lower() for k in ['tip', 'transportation', 'improvement']):
            # Could fetch and parse the PDF here
            pass
        
        # Look for project listings in the page
        project_match = re.search(r'(\d{5}[A-Z]?)', text)
        if project_match:
            project_id = project_match.group(1)
            
            # Get surrounding context
            parent = link.find_parent(['tr', 'li', 'div', 'p'])
            if parent:
                full_text = parent.get_text(strip=True)
                
                # Look for cost
                cost = None
                cost_match = re.search(r'\$([\d,]+)', full_text)
                if cost_match:
                    cost = parse_currency(cost_match.group(1))
                
                description = re.sub(r'\s+', ' ', full_text)[:200]
                
                lettings.append({
                    'id': generate_id(f"NH-RPC-{project_id}"),
                    'state': 'NH',
                    'project_id': project_id,
                    'description': f"{region}: {description}",
                    'cost_low': int(cost) if cost else None,
                    'cost_high': int(cost) if cost else None,
                    'cost_display': format_currency(cost) if cost else 'TBD',
                    'ad_date': None,
                    'let_date': None,
                    'project_type': classify_project_type(description),
                    'location': region,
                    'district': None,
                    'url': url,
                    'source': f'{rpc_name} TIP',
                    'business_lines': get_business_lines(description)
                })
    
    return lettings


def parse_municipal_bids(html: str, url: str, muni_name: str) -> List[Dict]:
    """Parse municipal bid page for construction opportunities."""
    lettings = []
    soup = BeautifulSoup(html, 'html.parser')
    
    # Common patterns for municipal bid listings
    # Look for tables first
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 2:
                continue
            
            text = ' '.join(c.get_text(strip=True) for c in cells)
            
            # Filter for construction-related bids
            if not any(kw in text.lower() for kw in ['paving', 'road', 'construction', 'highway', 
                                                      'bridge', 'infrastructure', 'sidewalk', 
                                                      'drainage', 'sewer', 'water']):
                continue
            
            # Look for bid number/ID
            bid_match = re.search(r'(RFP|RFQ|ITB|BID)[\s#-]*(\d+[\w-]*)', text, re.I)
            bid_id = bid_match.group(0) if bid_match else None
            
            # Look for cost/estimate
            cost = None
            cost_match = re.search(r'\$([\d,]+)', text)
            if cost_match:
                cost = parse_currency(cost_match.group(1))
            
            description = re.sub(r'\s+', ' ', text)[:200]
            
            lettings.append({
                'id': generate_id(f"NH-{muni_name}-{bid_id or description[:20]}"),
                'state': 'NH',
                'project_id': bid_id,
                'description': f"{muni_name}: {description}",
                'cost_low': int(cost) if cost else None,
                'cost_high': int(cost) if cost else None,
                'cost_display': format_currency(cost) if cost else 'See Bid Docs',
                'ad_date': None,
                'let_date': None,
                'project_type': classify_project_type(description),
                'location': muni_name,
                'district': None,
                'url': url,
                'source': f'{muni_name} Municipal',
                'business_lines': get_business_lines(description)
            })
    
    # Also look for list items
    list_items = soup.find_all(['li', 'div'], class_=lambda x: x and 'bid' in str(x).lower())
    
    for item in list_items:
        text = item.get_text(strip=True)
        
        if not any(kw in text.lower() for kw in ['paving', 'road', 'construction', 'highway', 
                                                  'bridge', 'infrastructure']):
            continue
        
        bid_match = re.search(r'(RFP|RFQ|ITB|BID)[\s#-]*(\d+[\w-]*)', text, re.I)
        bid_id = bid_match.group(0) if bid_match else None
        
        cost = None
        cost_match = re.search(r'\$([\d,]+)', text)
        if cost_match:
            cost = parse_currency(cost_match.group(1))
        
        description = re.sub(r'\s+', ' ', text)[:200]
        
        lettings.append({
            'id': generate_id(f"NH-{muni_name}-{bid_id or description[:20]}"),
            'state': 'NH',
            'project_id': bid_id,
            'description': f"{muni_name}: {description}",
            'cost_low': int(cost) if cost else None,
            'cost_high': int(cost) if cost else None,
            'cost_display': format_currency(cost) if cost else 'See Bid Docs',
            'ad_date': None,
            'let_date': None,
            'project_type': classify_project_type(description),
            'location': muni_name,
            'district': None,
            'url': url,
            'source': f'{muni_name} Municipal',
            'business_lines': get_business_lines(description)
        })
    
    return lettings


def classify_project_type(text: str) -> str:
    """Classify project type from description."""
    text_lower = text.lower()
    
    if any(k in text_lower for k in ['bridge', 'culvert', 'span']):
        return 'Bridge'
    elif any(k in text_lower for k in ['paving', 'resurfacing', 'overlay', 'pavement', 'asphalt']):
        return 'Pavement'
    elif any(k in text_lower for k in ['i-93', 'i-89', 'i-95', 'interstate', 'turnpike']):
        return 'Highway'
    elif any(k in text_lower for k in ['signal', 'intersection', 'safety']):
        return 'Safety'
    elif any(k in text_lower for k in ['sidewalk', 'pedestrian', 'bike', 'trail']):
        return 'Multimodal'
    else:
        return 'Highway'


# =============================================================================
# PORTAL STUBS
# =============================================================================

def create_portal_stub(state: str) -> Dict:
    cfg = DOT_SOURCES[state]
    return {
        'id': generate_id(f"{state}-portal"),
        'state': state,
        'project_id': None,
        'description': f"{cfg['name']} Bid Schedule - Visit portal for current lettings",
        'cost_low': None, 'cost_high': None, 'cost_display': 'See Portal',
        'ad_date': None, 'let_date': None,
        'project_type': None, 'location': None, 'district': None,
        'url': cfg['portal_url'],
        'source': cfg['name'],
        'business_lines': ['highway']
    }


# =============================================================================
# FETCH FUNCTIONS
# =============================================================================

def fetch_dot_lettings() -> List[Dict]:
    lettings = []
    for state, cfg in DOT_SOURCES.items():
        print(f"  🏗️ {cfg['name']} ({state})...")
        try:
            if cfg['parser'] == 'active' and state == 'MA':
                lettings.extend(parse_massdot())
            elif cfg['parser'] == 'active' and state == 'ME':
                lettings.extend(parse_mainedot())
            elif cfg['parser'] == 'active' and state == 'NH':
                lettings.extend(parse_nhdot())
            else:
                lettings.append(create_portal_stub(state))
                print(f"    ✓ Portal link")
        except Exception as e:
            print(f"    ✗ {e}")
            lettings.append(create_portal_stub(state))
    return lettings


def fetch_rss_feeds() -> List[Dict]:
    news = []
    for source, cfg in RSS_FEEDS.items():
        try:
            print(f"  📰 {source}...")
            feed = feedparser.parse(cfg['url'], request_headers={'User-Agent': 'NECMIS/3.0'})
            count = 0
            for entry in feed.entries[:20]:
                title = entry.get('title', '')
                summary = entry.get('summary', entry.get('description', ''))
                link = entry.get('link', '')
                
                if summary:
                    summary = BeautifulSoup(summary, 'html.parser').get_text()[:300].strip()
                
                combined = f"{title} {summary}"
                if not is_construction_relevant(combined):
                    continue
                
                pub = entry.get('published_parsed') or entry.get('updated_parsed')
                date_str = datetime(*pub[:6]).strftime('%Y-%m-%d') if pub else datetime.now().strftime('%Y-%m-%d')
                
                funding_kw = ['grant', 'funding', 'award', 'federal', 'million', 'billion', '$']
                category = 'funding' if any(k in combined.lower() for k in funding_kw) else 'news'
                
                news.append({
                    'id': generate_id(link or title),
                    'title': title,
                    'summary': summary,
                    'url': link,
                    'source': source,
                    'state': cfg['state'],
                    'date': date_str,
                    'category': category,
                    'priority': get_priority(combined),
                    'business_lines': get_business_lines(combined)
                })
                count += 1
            print(f"    ✓ {count} items")
        except Exception as e:
            print(f"    ✗ {e}")
    
    news.sort(key=lambda x: x['date'], reverse=True)
    return news


# =============================================================================
# MARKET HEALTH & SUMMARY
# =============================================================================

def calculate_market_health(dot_lettings: List[Dict], news: List[Dict]) -> Dict:
    total_value = sum(d.get('cost_low') or 0 for d in dot_lettings)
    
    if total_value >= 100000000:
        dot_score, dot_trend, dot_action = 9.0, 'up', 'Expand highway capacity - strong pipeline'
    elif total_value >= 50000000:
        dot_score, dot_trend, dot_action = 8.2, 'up', 'Expand highway capacity'
    elif total_value >= 20000000:
        dot_score, dot_trend, dot_action = 7.0, 'stable', 'Maintain position'
    elif total_value > 0:
        dot_score, dot_trend, dot_action = 6.0, 'stable', 'Monitor opportunities'
    else:
        dot_score, dot_trend, dot_action = 8.2, 'up', 'Expand highway capacity'
    
    mh = {
        'dot_pipeline': {'score': dot_score, 'trend': dot_trend, 'action': dot_action},
        'housing_permits': {'score': 6.5, 'trend': 'stable', 'action': 'Monitor trends'},
        'construction_spending': {'score': 6.1, 'trend': 'down', 'action': 'Selective investment'},
        'migration': {'score': 7.3, 'trend': 'up', 'action': 'Geographic expansion'},
        'input_cost_stability': {'score': 5.5, 'trend': 'down', 'action': 'Hedge 6 months'},
        'infrastructure_funding': {'score': 7.8, 'trend': 'stable', 'action': 'Selective growth'}
    }
    
    weights = {'dot_pipeline': 0.15, 'housing_permits': 0.10, 'construction_spending': 0.10,
               'migration': 0.10, 'input_cost_stability': 0.08, 'infrastructure_funding': 0.07}
    
    total_w = sum(mh[k]['score'] * weights[k] for k in weights)
    sum_w = sum(weights.values())
    overall = round(total_w / sum_w, 1)
    
    status = 'growth' if overall >= 7.5 else 'stable' if overall >= 6.0 else 'watchlist'
    mh['overall_score'] = overall
    mh['overall_status'] = status
    
    return mh


def build_summary(dot_lettings: List[Dict], news: List[Dict]) -> Dict:
    total_low = sum(d.get('cost_low') or 0 for d in dot_lettings)
    total_high = sum(d.get('cost_high') or 0 for d in dot_lettings)
    
    by_state = {s: 0 for s in STATES}
    for d in dot_lettings:
        if d['state'] in by_state:
            by_state[d['state']] += 1
    for n in news:
        if n['state'] in by_state:
            by_state[n['state']] += 1
    
    by_cat = {
        'dot_letting': len(dot_lettings),
        'news': len([n for n in news if n['category'] == 'news']),
        'funding': len([n for n in news if n['category'] == 'funding'])
    }
    
    return {
        'total_opportunities': by_cat['dot_letting'] + by_cat['funding'],
        'total_value_low': total_low,
        'total_value_high': total_high,
        'by_state': by_state,
        'by_category': by_cat
    }


# =============================================================================
# MAIN
# =============================================================================

def run_scraper() -> Dict:
    print("=" * 60)
    print("NECMIS SCRAPER - PHASE 3.0 (Dynamic NHDOT Parser)")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    print("[1/3] DOT Bid Schedules...")
    dot_lettings = fetch_dot_lettings()
    with_cost = len([d for d in dot_lettings if d.get('cost_low')])
    with_details = len([d for d in dot_lettings if d.get('project_type') or d.get('location')])
    total_val = sum(d.get('cost_low') or 0 for d in dot_lettings)
    print(f"  Total: {len(dot_lettings)} ({with_cost} with $, {with_details} with details)")
    print(f"  Pipeline: {format_currency(total_val)}")
    print()
    
    print("[2/3] RSS Feeds...")
    news = fetch_rss_feeds()
    print(f"  Total: {len(news)} items")
    print()
    
    print("[3/3] Market Health...")
    mh = calculate_market_health(dot_lettings, news)
    print(f"  Score: {mh['overall_score']}/10 ({mh['overall_status'].upper()})")
    print(f"  DOT Pipeline: {mh['dot_pipeline']['score']}/10")
    print()
    
    summary = build_summary(dot_lettings, news)
    
    data = {
        'generated': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'summary': summary,
        'dot_lettings': dot_lettings,
        'news': news,
        'market_health': mh
    }
    
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Pipeline: {format_currency(summary['total_value_low'])}")
    print(f"DOT Lettings: {summary['by_category']['dot_letting']} ({with_cost} with $)")
    print(f"News: {summary['by_category']['news']}")
    print(f"Funding: {summary['by_category']['funding']}")
    
    print("\nBy State:")
    for state in ['MA', 'ME', 'NH']:
        state_projects = [d for d in dot_lettings if d['state'] == state]
        state_value = sum(d.get('cost_low') or 0 for d in state_projects)
        print(f"  {state}: {len(state_projects)} projects, {format_currency(state_value)}")
    
    print("=" * 60)
    
    return data


if __name__ == '__main__':
    data = run_scraper()
    os.makedirs('data', exist_ok=True)
    with open('data/necmis_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("✓ Saved to data/necmis_data.json")
