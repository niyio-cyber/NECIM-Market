#!/usr/bin/env python3
"""
NECMIS Scraper - Phase 3 (Multi-State Northeast)
=================================================
Full Parsers: MassDOT, MaineDOT, VTrans
Enhanced Stub: NHDOT (site blocks scrapers - uses known project data)
Portal Stubs: NYSDOT, RIDOT, CTDOT, PennDOT

Target States: VT, NH, ME, MA (core), NY, RI, CT, PA (extended)
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
CORE_STATES = ['VT', 'NH', 'ME', 'MA']  # Primary focus

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
    'Hartford Courant': {'url': 'https://www.courant.com/arcio/rss/', 'state': 'CT'}
}

DOT_SOURCES = {
    'MA': {
        'name': 'MassDOT',
        'portal_url': 'https://hwy.massdot.state.ma.us/webapps/const/statusReport.asp',
        'parser': 'massdot'
    },
    'ME': {
        'name': 'MaineDOT',
        'portal_url': 'https://www.maine.gov/dot/doing-business/bid-opportunities',
        'parser': 'mainedot'
    },
    'VT': {
        'name': 'VTrans',
        'portal_url': 'https://vtrans.vermont.gov/contract-admin/results-awards/construction-contracting/historical/2025',
        'bid_url': 'https://vtrans.vermont.gov/contract-admin/bids-requests/construction-contracting',
        'parser': 'vtrans'
    },
    'NH': {
        'name': 'NHDOT',
        'portal_url': 'https://www.dot.nh.gov/doing-business-nhdot/contractors/invitation-bid',
        'parser': 'nhdot_enhanced'  # Uses known data since site blocks scrapers
    },
    'NY': {
        'name': 'NYSDOT',
        'portal_url': 'https://www.dot.ny.gov/doing-business/opportunities/const-highway',
        'parser': 'stub'
    },
    'RI': {
        'name': 'RIDOT',
        'portal_url': 'https://www.dot.ri.gov/about/current_projects.php',
        'parser': 'stub'
    },
    'CT': {
        'name': 'CTDOT',
        'portal_url': 'https://portal.ct.gov/DOT/Doing-Business/Contractor-Information',
        'parser': 'stub'
    },
    'PA': {
        'name': 'PennDOT',
        'portal_url': 'https://www.penndot.pa.gov/business/Letting/Pages/default.aspx',
        'parser': 'stub'
    }
}

CONSTRUCTION_KEYWORDS = {
    'high_priority': ['highway', 'bridge', 'DOT', 'bid', 'letting', 'RFP', 'contract award',
                      'paving', 'resurfacing', 'infrastructure', 'IIJA', 'federal grant',
                      'reconstruction', 'rehabilitation'],
    'medium_priority': ['construction', 'road', 'pavement', 'asphalt', 'concrete',
                        'aggregate', 'gravel', 'development', 'permit', 'municipal',
                        'culvert', 'drainage', 'signal'],
    'business_line_keywords': {
        'highway': ['highway', 'road', 'interstate', 'route', 'bridge', 'DOT',
                    'transportation', 'reconstruction', 'resurfacing', 'pavement',
                    'rehabilitation', 'guardrail'],
        'hma': ['asphalt', 'paving', 'resurfacing', 'overlay', 'milling', 'HMA',
                'hot mix', 'surfacing', 'cyclical', 'wearing course', 'bonded'],
        'aggregates': ['aggregate', 'gravel', 'sand', 'stone', 'quarry', 'crushed'],
        'ready_mix': ['concrete', 'ready-mix', 'cement', 'bridge deck', 'deck',
                      'sidewalk', 'curbing'],
        'liquid_asphalt': ['liquid asphalt', 'bitumen', 'emulsion', 'binder']
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


def get_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }


# =============================================================================
# MASSDOT PARSER (Full - Working)
# =============================================================================

def parse_massdot() -> List[Dict]:
    """Parse MassDOT Construction Status Report - extracts full project details."""
    url = DOT_SOURCES['MA']['portal_url']
    lettings = []
    
    try:
        print(f"    🔍 Fetching MassDOT...")
        response = requests.get(url, timeout=30, headers=get_headers())
        response.raise_for_status()
        html = response.text
        
        print(f"    📄 Got {len(html)} bytes")
        
        # Convert to plain text for reliable parsing
        soup = BeautifulSoup(html, 'html.parser')
        for script in soup(["script", "style"]):
            script.decompose()
        text = soup.get_text(separator='\n')
        text = re.sub(r'\n\s*\n', '\n', text)
        
        print(f"    📝 Converted to {len(text)} chars")
        
        # Split into project blocks
        blocks = re.split(r'(?=Location:)', text)
        print(f"    📦 Found {len(blocks)} blocks")
        
        projects = []
        for block in blocks:
            if 'Project Value:' not in block:
                continue
            
            # Extract all fields
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
        
        print(f"    📊 Extracted {len(projects)} projects")
        
        # Build letting records
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
            
            project_url = url
            if p['project_num']:
                project_url = f"{url}?projnum={p['project_num']}"
            
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
            print(f"    ✓ {len(lettings)} projects, {format_currency(total)} total")
        else:
            print(f"    ⚠ No projects parsed")
            lettings.append(create_portal_stub('MA'))
            
    except Exception as e:
        print(f"    ✗ Error: {e}")
        lettings.append(create_portal_stub('MA'))
    
    return lettings


# =============================================================================
# MAINEDOT PARSER (Table - Working)
# =============================================================================

def parse_mainedot() -> List[Dict]:
    """Parse MaineDOT bid opportunities table."""
    url = DOT_SOURCES['ME']['portal_url']
    lettings = []
    
    try:
        print(f"    🔍 Fetching MaineDOT...")
        response = requests.get(url, timeout=30, headers=get_headers())
        response.raise_for_status()
        html = response.text
        
        print(f"    📄 Got {len(html)} bytes")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the bid table
        tables = soup.find_all('table')
        print(f"    📋 Found {len(tables)} tables")
        
        projects = []
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 4:
                    bid_date_text = cells[0].get_text(strip=True) if len(cells) > 0 else None
                    win_cell = cells[1] if len(cells) > 1 else None
                    win_text = win_cell.get_text(strip=True) if win_cell else None
                    municipality = cells[2].get_text(strip=True) if len(cells) > 2 else None
                    summary = cells[3].get_text(strip=True) if len(cells) > 3 else None
                    
                    # Get link
                    link = win_cell.find('a') if win_cell else None
                    href = link.get('href') if link else None
                    if href and not href.startswith('http'):
                        project_url = 'https://www.maine.gov' + href
                    elif href:
                        project_url = href
                    else:
                        project_url = url
                    
                    if win_text and municipality and summary:
                        bid_date = None
                        if bid_date_text:
                            try:
                                bid_date = datetime.strptime(bid_date_text, '%m/%d/%Y').strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        projects.append({
                            'win': win_text,
                            'municipality': municipality,
                            'summary': summary,
                            'bid_date': bid_date,
                            'url': project_url
                        })
        
        print(f"    📊 Extracted {len(projects)} projects from table")
        
        # Build letting records
        for p in projects[:30]:
            desc = p['summary'][:200] if p['summary'] else "MaineDOT Project"
            location = p['municipality']
            
            # Determine project type from description
            proj_type = None
            desc_lower = desc.lower()
            if 'bridge' in desc_lower:
                proj_type = 'Bridge'
            elif any(k in desc_lower for k in ['pavement', 'resurfacing', 'overlay', 'cyclical']):
                proj_type = 'Pavement'
            elif 'signal' in desc_lower:
                proj_type = 'Traffic Signals'
            elif 'culvert' in desc_lower:
                proj_type = 'Culvert'
            elif 'highway' in desc_lower:
                proj_type = 'Highway'
            elif any(k in desc_lower for k in ['crew quarters', 'building']):
                proj_type = 'Building'
            
            # Extract region from municipality if present
            region_match = re.search(r'Region\s*(\d+)', location, re.IGNORECASE)
            district = int(region_match.group(1)) if region_match else None
            
            lettings.append({
                'id': generate_id(f"ME-{p['win']}-{desc[:25]}"),
                'state': 'ME',
                'project_id': p['win'],
                'description': desc,
                'cost_low': None,  # MaineDOT doesn't show costs in main listing
                'cost_high': None,
                'cost_display': 'See Bid Docs',
                'ad_date': None,
                'let_date': p['bid_date'],
                'project_type': proj_type,
                'location': location,
                'district': district,
                'url': p['url'],
                'source': 'MaineDOT',
                'business_lines': get_business_lines(desc)
            })
        
        if lettings:
            print(f"    ✓ {len(lettings)} projects (costs in bid docs)")
        else:
            print(f"    ⚠ No projects parsed")
            lettings.append(create_portal_stub('ME'))
            
    except Exception as e:
        print(f"    ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        lettings.append(create_portal_stub('ME'))
    
    return lettings


# =============================================================================
# VTRANS PARSER (Bid Results - New)
# =============================================================================

def parse_vtrans() -> List[Dict]:
    """Parse VTrans bid results table - shows awarded contracts with amounts."""
    url = DOT_SOURCES['VT']['portal_url']
    lettings = []
    
    try:
        print(f"    🔍 Fetching VTrans...")
        response = requests.get(url, timeout=30, headers=get_headers())
        response.raise_for_status()
        html = response.text
        
        print(f"    📄 Got {len(html)} bytes")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the bid results table
        tables = soup.find_all('table')
        print(f"    📋 Found {len(tables)} tables")
        
        projects = []
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 5:
                    contract_num = cells[0].get_text(strip=True) if len(cells) > 0 else None
                    project_name = cells[1].get_text(strip=True) if len(cells) > 1 else None
                    bid_date = cells[2].get_text(strip=True) if len(cells) > 2 else None
                    details_cell = cells[3] if len(cells) > 3 else None
                    award_cell = cells[4] if len(cells) > 4 else None
                    
                    # Extract award amount from cell text
                    award_text = award_cell.get_text(strip=True) if award_cell else ''
                    contractor = None
                    
                    # Look for dollar amount in award text
                    amount_match = re.search(r'\$([0-9,]+(?:\.\d{2})?)', award_text)
                    cost = None
                    if amount_match:
                        cost = parse_currency(amount_match.group(1))
                    
                    # Extract contractor name
                    contractor_match = re.search(r'([A-Z][A-Za-z\.\s&,]+(?:Inc|LLC|Corp|Co|Construction|Contractors|Brothers)\.?)', award_text)
                    if contractor_match:
                        contractor = contractor_match.group(1).strip()
                    
                    # Get PDF link if available
                    pdf_link = details_cell.find('a') if details_cell else None
                    pdf_url = None
                    if pdf_link and pdf_link.get('href'):
                        href = pdf_link.get('href')
                        if not href.startswith('http'):
                            pdf_url = 'https://vtrans.vermont.gov' + href
                        else:
                            pdf_url = href
                    
                    if contract_num and project_name and project_name != 'N/A':
                        # Parse bid date
                        bid_date_parsed = None
                        if bid_date:
                            try:
                                bid_date_parsed = datetime.strptime(bid_date, '%m/%d/%y').strftime('%Y-%m-%d')
                            except:
                                pass
                        
                        # Extract location from project name
                        # Format: "BARRE TOWN STP 6100 (15)" or "COLCHESTER-ESSEX NH PS24 (11)"
                        location_match = re.match(r'^([A-Z][A-Z\-\s]+?)(?:\s+(?:STP|NH|IM|BF|BO|GMRC|RELV|CMG|HES|AV))', project_name)
                        location = location_match.group(1).strip().title() if location_match else None
                        
                        projects.append({
                            'contract_num': contract_num,
                            'project_name': project_name,
                            'location': location,
                            'bid_date': bid_date_parsed,
                            'cost': cost,
                            'contractor': contractor,
                            'pdf_url': pdf_url
                        })
        
        print(f"    📊 Extracted {len(projects)} awarded contracts")
        
        # Build letting records (most recent first)
        for p in projects[:25]:
            desc = p['project_name']
            if p['contractor']:
                desc = f"{p['project_name']} - Awarded to {p['contractor']}"
            
            # Determine project type
            proj_type = None
            name_upper = p['project_name'].upper()
            if 'BF' in name_upper or 'BRIDGE' in name_upper:
                proj_type = 'Bridge'
            elif 'STP' in name_upper or 'FPAV' in name_upper:
                proj_type = 'Pavement'
            elif 'IM' in name_upper:
                proj_type = 'Interstate'
            elif 'GMRC' in name_upper:
                proj_type = 'Green Mountain Railroad'
            elif 'CULV' in name_upper:
                proj_type = 'Culvert'
            elif 'MARK' in name_upper:
                proj_type = 'Pavement Marking'
            elif 'HES' in name_upper or 'HRRR' in name_upper:
                proj_type = 'Safety'
            elif 'AV' in name_upper:
                proj_type = 'Aviation'
            
            lettings.append({
                'id': generate_id(f"VT-{p['contract_num']}-{p['project_name'][:25]}"),
                'state': 'VT',
                'project_id': p['contract_num'],
                'description': desc[:200],
                'cost_low': int(p['cost']) if p['cost'] else None,
                'cost_high': int(p['cost']) if p['cost'] else None,
                'cost_display': format_currency(p['cost']) if p['cost'] else 'See Results',
                'ad_date': None,
                'let_date': p['bid_date'],
                'project_type': proj_type,
                'location': p['location'],
                'district': None,
                'url': p['pdf_url'] or DOT_SOURCES['VT']['bid_url'],
                'source': 'VTrans',
                'business_lines': get_business_lines(p['project_name']),
                'status': 'awarded',
                'contractor': p['contractor']
            })
        
        if lettings:
            total = sum(l.get('cost_low') or 0 for l in lettings)
            with_cost = len([l for l in lettings if l.get('cost_low')])
            print(f"    ✓ {len(lettings)} contracts ({with_cost} with $, {format_currency(total)} total)")
        else:
            print(f"    ⚠ No contracts parsed")
            lettings.append(create_portal_stub('VT'))
            
    except Exception as e:
        print(f"    ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        lettings.append(create_portal_stub('VT'))
    
    return lettings


# =============================================================================
# NHDOT ENHANCED STUB (Site blocks scrapers - uses known data)
# =============================================================================

def parse_nhdot_enhanced() -> List[Dict]:
    """
    NHDOT blocks automated requests (403 error).
    This uses known project data from search results and public information.
    Projects are marked as 'verify' status - users should confirm at portal.
    """
    print(f"    🔍 NHDOT (enhanced stub - site blocks scrapers)...")
    
    # Known projects from search results (January 2026)
    # Source: NHDOT Invitation for Bid page via web search
    known_projects = [
        {
            'project_id': '45074',
            'location': 'Allenstown-Pembroke-Epsom',
            'description': 'Pavement resurfacing on four sections in five Towns in Districts 3 & 5',
            'cost': 10230532,
            'let_date': '2025-11-06',
            'project_type': 'Pavement'
        },
        {
            'project_id': '45075',
            'location': 'Statewide',
            'description': 'Statewide Tier 2 Resurfacing (Central)',
            'cost': None,  # Combined with 45074
            'let_date': '2025-11-06',
            'project_type': 'Pavement'
        },
        {
            'project_id': '45073',
            'location': 'Belmont-Gilford-Tilton',
            'description': 'Pavement resurfacing on two sections in four towns in District 3',
            'cost': 5583159,
            'let_date': '2025-11-13',
            'project_type': 'Pavement'
        },
        {
            'project_id': '40514',
            'location': 'Franconia',
            'description': 'Pavement rehabilitation along I-93 in Franconia',
            'cost': 31273556,
            'let_date': '2025-11-06',
            'project_type': 'Interstate'
        },
        {
            'project_id': '43071A',
            'location': 'Manchester-Hooksett',
            'description': 'Pavement resurfacing on one section along I-93 northbound in District 5',
            'cost': None,
            'let_date': None,
            'project_type': 'Interstate'
        },
        {
            'project_id': '44822',
            'location': 'Pittsburg',
            'description': 'Replace 19,000 feet of deficient cable and W-beam guardrail with 23,100 feet of new beam guardrail on US 3',
            'cost': None,
            'let_date': '2025-10-23',
            'project_type': 'Safety'
        },
        {
            'project_id': '45027',
            'location': 'North Hampton-Greenland-Portsmouth',
            'description': 'Highway construction and improvements',
            'cost': 4918627,
            'let_date': None,
            'project_type': 'Highway'
        },
        {
            'project_id': '40392',
            'location': 'Andover',
            'description': 'Replacement of US 4 Bridge over Blackwater River and reconstruction of 1100 feet of US 4',
            'cost': None,
            'let_date': None,
            'project_type': 'Bridge'
        }
    ]
    
    lettings = []
    portal_url = DOT_SOURCES['NH']['portal_url']
    
    for p in known_projects:
        lettings.append({
            'id': generate_id(f"NH-{p['project_id']}-{p['description'][:25]}"),
            'state': 'NH',
            'project_id': p['project_id'],
            'description': p['description'],
            'cost_low': p['cost'],
            'cost_high': p['cost'],
            'cost_display': format_currency(p['cost']) if p['cost'] else 'See Portal',
            'ad_date': None,
            'let_date': p['let_date'],
            'project_type': p['project_type'],
            'location': p['location'],
            'district': None,
            'url': portal_url,
            'source': 'NHDOT',
            'business_lines': get_business_lines(p['description']),
            'status': 'verify'  # Flag that this should be verified at portal
        })
    
    total = sum(l.get('cost_low') or 0 for l in lettings)
    with_cost = len([l for l in lettings if l.get('cost_low')])
    print(f"    ✓ {len(lettings)} known projects ({with_cost} with $, {format_currency(total)})")
    print(f"    ⚠ Note: NHDOT blocks scrapers - verify at portal")
    
    return lettings


# =============================================================================
# PORTAL STUB
# =============================================================================

def create_portal_stub(state: str) -> Dict:
    cfg = DOT_SOURCES[state]
    return {
        'id': generate_id(f"{state}-portal"),
        'state': state,
        'project_id': None,
        'description': f"{cfg['name']} Bid Schedule - Visit portal for current lettings",
        'cost_low': None,
        'cost_high': None,
        'cost_display': 'See Portal',
        'ad_date': None,
        'let_date': None,
        'project_type': None,
        'location': None,
        'district': None,
        'url': cfg['portal_url'],
        'source': cfg['name'],
        'business_lines': ['highway']
    }


# =============================================================================
# DOT & RSS FETCHING
# =============================================================================

def fetch_dot_lettings() -> List[Dict]:
    lettings = []
    for state, cfg in DOT_SOURCES.items():
        print(f"  🏗️ {cfg['name']} ({state})...")
        try:
            if cfg['parser'] == 'massdot':
                lettings.extend(parse_massdot())
            elif cfg['parser'] == 'mainedot':
                lettings.extend(parse_mainedot())
            elif cfg['parser'] == 'vtrans':
                lettings.extend(parse_vtrans())
            elif cfg['parser'] == 'nhdot_enhanced':
                lettings.extend(parse_nhdot_enhanced())
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
    projects_with_cost = len([d for d in dot_lettings if d.get('cost_low')])
    total_projects = len([d for d in dot_lettings if d.get('project_id')])
    
    # Core states only for scoring
    core_lettings = [d for d in dot_lettings if d['state'] in CORE_STATES]
    core_value = sum(d.get('cost_low') or 0 for d in core_lettings)
    core_projects = len([d for d in core_lettings if d.get('project_id')])
    
    # Score based on value and project count
    if core_value >= 200000000:
        dot_score, dot_trend, dot_action = 9.5, 'up', 'Aggressive expansion - exceptional pipeline'
    elif core_value >= 100000000:
        dot_score, dot_trend, dot_action = 9.0, 'up', 'Expand highway capacity - strong pipeline'
    elif core_value >= 50000000:
        dot_score, dot_trend, dot_action = 8.2, 'up', 'Expand highway capacity'
    elif core_value >= 20000000 or core_projects >= 30:
        dot_score, dot_trend, dot_action = 7.5, 'up', 'Strong project pipeline'
    elif core_value > 0 or core_projects >= 15:
        dot_score, dot_trend, dot_action = 7.0, 'stable', 'Maintain position'
    else:
        dot_score, dot_trend, dot_action = 6.5, 'stable', 'Monitor opportunities'
    
    mh = {
        'dot_pipeline': {'score': dot_score, 'trend': dot_trend, 'action': dot_action},
        'housing_permits': {'score': 6.5, 'trend': 'stable', 'action': 'Monitor trends'},
        'construction_spending': {'score': 6.1, 'trend': 'down', 'action': 'Selective investment'},
        'migration': {'score': 7.3, 'trend': 'up', 'action': 'Geographic expansion'},
        'input_cost_stability': {'score': 5.5, 'trend': 'down', 'action': 'Hedge 6 months'},
        'infrastructure_funding': {'score': 7.8, 'trend': 'stable', 'action': 'Selective growth'}
    }
    
    weights = {
        'dot_pipeline': 0.15,
        'housing_permits': 0.10,
        'construction_spending': 0.10,
        'migration': 0.10,
        'input_cost_stability': 0.08,
        'infrastructure_funding': 0.07
    }
    
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
    
    by_state = {s: {'projects': 0, 'value': 0} for s in STATES}
    for d in dot_lettings:
        if d['state'] in by_state:
            by_state[d['state']]['projects'] += 1
            by_state[d['state']]['value'] += d.get('cost_low') or 0
    for n in news:
        if n['state'] in by_state:
            pass  # News doesn't add to project count
    
    by_cat = {
        'dot_letting': len([d for d in dot_lettings if d.get('project_id')]),
        'news': len([n for n in news if n['category'] == 'news']),
        'funding': len([n for n in news if n['category'] == 'funding'])
    }
    
    # Core vs extended breakdown
    core = {s: by_state[s] for s in CORE_STATES}
    extended = {s: by_state[s] for s in STATES if s not in CORE_STATES}
    
    return {
        'total_opportunities': by_cat['dot_letting'] + by_cat['funding'],
        'total_value_low': total_low,
        'total_value_high': total_high,
        'by_state': by_state,
        'by_category': by_cat,
        'core_states': core,
        'extended_states': extended
    }


# =============================================================================
# MAIN
# =============================================================================

def run_scraper() -> Dict:
    print("=" * 70)
    print("NECMIS SCRAPER - PHASE 3 (Multi-State Northeast)")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Core States: {', '.join(CORE_STATES)}")
    print(f"Extended States: {', '.join([s for s in STATES if s not in CORE_STATES])}")
    print()
    
    print("[1/3] DOT Bid Schedules...")
    dot_lettings = fetch_dot_lettings()
    with_cost = len([d for d in dot_lettings if d.get('cost_low')])
    with_id = len([d for d in dot_lettings if d.get('project_id')])
    total_val = sum(d.get('cost_low') or 0 for d in dot_lettings)
    print(f"  Total: {len(dot_lettings)} ({with_cost} with $, {with_id} with project IDs)")
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
        'version': '3.0',
        'summary': summary,
        'dot_lettings': dot_lettings,
        'news': news,
        'market_health': mh
    }
    
    # State breakdown
    print("=" * 70)
    print("SUMMARY BY STATE")
    print("=" * 70)
    for state in CORE_STATES:
        state_data = summary['by_state'][state]
        marker = "⭐" if state in CORE_STATES else "  "
        print(f"{marker} {state}: {state_data['projects']} projects, {format_currency(state_data['value'])}")
    print("-" * 35)
    for state in [s for s in STATES if s not in CORE_STATES]:
        state_data = summary['by_state'][state]
        print(f"   {state}: {state_data['projects']} projects")
    print("=" * 70)
    print(f"TOTAL PIPELINE: {format_currency(summary['total_value_low'])}")
    print(f"News: {summary['by_category']['news']}, Funding: {summary['by_category']['funding']}")
    print("=" * 70)
    
    return data


if __name__ == '__main__':
    data = run_scraper()
    os.makedirs('data', exist_ok=True)
    with open('data/necmis_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("✓ Saved to data/necmis_data.json")
