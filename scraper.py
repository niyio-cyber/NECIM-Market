#!/usr/bin/env python3
"""
NECMIS Scraper - Phase 2.1 (Plain Text Parser)
===============================================
Converts HTML to plain text, then extracts project details.
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
    'Hartford Courant': {'url': 'https://www.courant.com/arcio/rss/', 'state': 'CT'}
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
# MASSDOT PARSER (Plain Text) - YOUR ORIGINAL WORKING CODE
# =============================================================================

def parse_massdot() -> List[Dict]:
    """
    Parse MassDOT by converting HTML to plain text first.
    """
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
        
        # Convert HTML to plain text using BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text with newlines preserved between elements
        text = soup.get_text(separator='\n')
        
        # Clean up multiple newlines
        text = re.sub(r'\n\s*\n', '\n', text)
        
        print(f"    📝 Converted to {len(text)} chars of text")
        
        # Debug: show sample
        sample = text[:800].replace('\n', ' | ')
        print(f"    🔬 Sample: {sample[:300]}...")
        
        # Extract using plain text patterns
        # Pattern: Look for Location: followed by text until Description:
        # Then Description: followed by text until District:
        
        # Split into project blocks - each starts with "Location:"
        blocks = re.split(r'(?=Location:)', text)
        print(f"    📦 Found {len(blocks)} potential project blocks")
        
        projects = []
        for block in blocks:
            if 'Project Value:' not in block:
                continue
            
            # Extract fields from this block
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
        
        # If block parsing didn't work, try line-by-line extraction
        if not projects:
            print(f"    🔄 Trying line-by-line extraction...")
            
            # Find all values first
            values = re.findall(r'Project Value:\s*\$([0-9,]+\.?\d*)', text)
            locations = re.findall(r'Location:\s*([A-Z][A-Za-z0-9\s\-,]+)', text)
            descriptions = re.findall(r'Description:\s*(.+?)(?=\s*District:|\n)', text)
            proj_nums = re.findall(r'Project Number:\s*(\d+)', text)
            proj_types = re.findall(r'Project Type:\s*([^\n]+)', text)
            ad_dates = re.findall(r'Ad Date:\s*(\d{1,2}/\d{1,2}/\d{4})', text)
            districts = re.findall(r'District:\s*(\d+)\s*Ad Date:', text)
            
            print(f"    Line extraction: {len(values)} val, {len(locations)} loc, {len(descriptions)} desc")
            
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
        
        # If still nothing, fallback to just dollar amounts
        if not projects:
            print(f"    🔄 Falling back to dollar-only extraction...")
            all_values = re.findall(r'\$([0-9,]+\.?\d*)', text)
            for i, v in enumerate(all_values):
                val = parse_currency(v)
                if val and 100000 <= val <= 500000000:
                    projects.append({
                        'location': None,
                        'description': f"MassDOT Project #{i+1}",
                        'value': v,
                        'project_num': None,
                        'project_type': None,
                        'ad_date': None,
                        'district': None
                    })
        
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
                proj_type = re.sub(r'\s*,\s*$', '', proj_type)
                proj_type = proj_type[:60]
            
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
            
            letting = {
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
            }
            lettings.append(letting)
        
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
# MAINEDOT PARSER - Multi-source with $$ values from CAP
# Priority: 1) Excel file, 2) PDF, 3) Portal stub
# =============================================================================

def parse_mainedot() -> List[Dict]:
    """
    Parse MaineDOT Construction Advertisement Plan (CAP).
    Primary: Excel file at https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.xls
    Backup: PDF at https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.pdf
    Note: HTML table uses DataTables (JS-loaded) - cannot be scraped without headless browser
    """
    lettings = []
    cap_url = "https://www.maine.gov/dot/major-projects/cap"
    excel_url = "https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.xls"
    pdf_url = "https://www.maine.gov/dot/sites/maine.gov.dot/files/inline-files/annual.pdf"
    
    # === ATTEMPT 1: Parse Excel file (BEST SOURCE - structured data) ===
    try:
        print(f"    🔍 Fetching MaineDOT CAP Excel file...")
        response = requests.get(excel_url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        print(f"    📊 Got Excel: {len(response.content)} bytes")
        
        try:
            import pandas as pd
            import io
            
            # Read Excel file
            df = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
            print(f"    📋 Excel has {len(df)} rows, columns: {list(df.columns)[:6]}...")
            
            # Expected columns (may vary): Work Type, Planned Advertise Date, Location/Title, Details, Project ID, Total Project Estimate
            # Try to find the right columns
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
            
            print(f"    🔬 Column mapping: {col_map}")
            
            for idx, row in df.iterrows():
                try:
                    work_type = str(row.get(col_map.get('work_type', ''), '')).strip()
                    ad_date_raw = row.get(col_map.get('ad_date', ''), '')
                    location = str(row.get(col_map.get('location', ''), '')).strip()
                    details = str(row.get(col_map.get('details', ''), '')).strip()
                    project_id = str(row.get(col_map.get('project_id', ''), '')).strip()
                    cost_raw = row.get(col_map.get('cost', ''), '')
                    
                    # Skip empty rows or header-like rows
                    if not location or location == 'nan' or 'Location' in location:
                        continue
                    
                    # Parse cost
                    cost = None
                    if pd.notna(cost_raw):
                        if isinstance(cost_raw, (int, float)):
                            cost = int(cost_raw)
                        else:
                            cost_match = re.search(r'\$?([\d,]+)', str(cost_raw))
                            if cost_match:
                                cost = int(cost_match.group(1).replace(',', ''))
                    
                    # Parse date
                    let_date = None
                    if pd.notna(ad_date_raw):
                        try:
                            if isinstance(ad_date_raw, datetime):
                                let_date = ad_date_raw.strftime('%Y-%m-%d')
                            else:
                                let_date = datetime.strptime(str(ad_date_raw), '%m/%d/%Y').strftime('%Y-%m-%d')
                        except:
                            pass
                    
                    # Extract description from details
                    description = location
                    if details and details != 'nan':
                        desc_match = re.search(r'Description:\s*(.+?)(?:Program Manager:|Phone:|$)', details, re.DOTALL)
                        if desc_match:
                            description = f"{location}: {desc_match.group(1).strip()[:150]}"
                    
                    # Map work type to project type
                    proj_type = None
                    wt_lower = work_type.lower()
                    if 'bridge' in wt_lower:
                        proj_type = 'Bridge'
                    elif 'paving' in wt_lower or 'preservation' in wt_lower or 'lcp' in wt_lower:
                        proj_type = 'Pavement'
                    elif 'highway' in wt_lower and 'construction' in wt_lower:
                        proj_type = 'Highway'
                    elif 'highway' in wt_lower and 'rehab' in wt_lower:
                        proj_type = 'Highway Rehab'
                    elif 'safety' in wt_lower or 'spot' in wt_lower:
                        proj_type = 'Safety'
                    elif 'multimodal' in wt_lower:
                        proj_type = 'Multimodal'
                    elif 'maintenance' in wt_lower:
                        proj_type = 'Maintenance'
                    else:
                        proj_type = work_type[:30] if work_type else None
                    
                    lettings.append({
                        'id': generate_id(f"ME-{project_id}-{location[:20]}"),
                        'state': 'ME',
                        'project_id': project_id if project_id != 'nan' else None,
                        'description': description[:200],
                        'cost_low': cost,
                        'cost_high': cost,
                        'cost_display': format_currency(cost) if cost else 'TBD',
                        'ad_date': let_date,
                        'let_date': let_date,
                        'project_type': proj_type,
                        'location': location.split(',')[0] if ',' in location else location,
                        'district': None,
                        'url': cap_url,
                        'source': 'MaineDOT CAP',
                        'business_lines': get_business_lines(f"{work_type} {location} {details}")
                    })
                    
                except Exception as e:
                    continue
            
            if lettings:
                # Deduplicate by project_id
                seen_ids = set()
                unique_lettings = []
                for l in lettings:
                    key = l['project_id'] or l['description'][:50]
                    if key not in seen_ids:
                        seen_ids.add(key)
                        unique_lettings.append(l)
                lettings = unique_lettings
                
                total = sum(l.get('cost_low') or 0 for l in lettings)
                with_cost = len([l for l in lettings if l.get('cost_low')])
                print(f"    ✓ {len(lettings)} projects from Excel ({with_cost} with $), {format_currency(total)} pipeline")
                return lettings
            else:
                print(f"    ⚠ No projects extracted from Excel")
                
        except ImportError as e:
            print(f"    ⚠ pandas/xlrd not installed: {e}")
        except Exception as e:
            print(f"    ⚠ Excel parse error: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"    ⚠ Excel fetch failed: {e}")
    
    # === ATTEMPT 2: Parse PDF (Primary reliable source) ===
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
                
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text() or ''
                    lines = text.split('\n')
                    
                    for line in lines:
                        line_stripped = line.strip()
                        
                        # Detect work type headers
                        if line_stripped in work_type_headers:
                            current_work_type = line_stripped
                            continue
                        
                        # Skip header rows and empty lines
                        if not line_stripped or 'Plan Advertise Date' in line or 'Total Project Estimate' in line:
                            continue
                        
                        # Look for lines with project ID pattern (6 digits.2 digits) AND cost ($X,XXX,XXX)
                        id_match = re.search(r'(\d{6}\.\d{2})', line)
                        cost_match = re.search(r'\$([\d,]+)', line)
                        
                        if id_match and cost_match and current_work_type:
                            project_id = id_match.group(1)
                            
                            # Parse cost
                            try:
                                cost = int(cost_match.group(1).replace(',', ''))
                            except:
                                cost = None
                            
                            # Extract date (MM/DD/YYYY at start of line)
                            date_match = re.search(r'^(\d{2}/\d{2}/\d{4})', line)
                            let_date = None
                            if date_match:
                                try:
                                    let_date = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                                except:
                                    pass
                            
                            # Extract location (between date and project ID)
                            location = line
                            if date_match:
                                location = location[len(date_match.group(0)):].strip()
                            # Remove project ID and cost from location
                            location = re.sub(r'\d{6}\.\d{2}', '', location)
                            location = re.sub(r'\$[\d,]+', '', location)
                            location = location.strip()
                            
                            # Map work type to project type
                            proj_type = None
                            if 'bridge' in current_work_type.lower():
                                proj_type = 'Bridge'
                            elif 'paving' in current_work_type.lower() or 'preservation' in current_work_type.lower():
                                proj_type = 'Pavement'
                            elif 'highway' in current_work_type.lower():
                                proj_type = 'Highway'
                            elif 'safety' in current_work_type.lower():
                                proj_type = 'Safety'
                            elif 'multimodal' in current_work_type.lower():
                                proj_type = 'Multimodal'
                            
                            # Only add if we have meaningful location data
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
                    # Deduplicate by project_id
                    seen_ids = set()
                    unique_lettings = []
                    for l in lettings:
                        if l['project_id'] not in seen_ids:
                            seen_ids.add(l['project_id'])
                            unique_lettings.append(l)
                    lettings = unique_lettings
                    
                    total = sum(l.get('cost_low') or 0 for l in lettings)
                    with_cost = len([l for l in lettings if l.get('cost_low')])
                    print(f"    ✓ {len(lettings)} projects from PDF ({with_cost} with $), {format_currency(total)} pipeline")
                    return lettings
                else:
                    print(f"    ⚠ No projects extracted from PDF")
                    
        except ImportError:
            print(f"    ⚠ pdfplumber not installed - trying PyPDF2...")
            
            # Fallback to PyPDF2
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
                    lines = text.split('\n')
                    
                    for line in lines:
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
                            location = re.sub(r'\$[\d,]+', '', location)
                            location = location.strip()
                            
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
                    unique_lettings = []
                    for l in lettings:
                        if l['project_id'] not in seen_ids:
                            seen_ids.add(l['project_id'])
                            unique_lettings.append(l)
                    lettings = unique_lettings
                    
                    total = sum(l.get('cost_low') or 0 for l in lettings)
                    with_cost = len([l for l in lettings if l.get('cost_low')])
                    print(f"    ✓ {len(lettings)} projects from PDF/PyPDF2 ({with_cost} with $), {format_currency(total)} pipeline")
                    return lettings
                    
            except ImportError:
                print(f"    ⚠ PyPDF2 also not installed - cannot parse PDF")
            except Exception as e:
                print(f"    ⚠ PyPDF2 parse error: {e}")
        except Exception as e:
            print(f"    ⚠ PDF parse error: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"    ⚠ PDF fetch failed: {e}")
    
    # === FALLBACK: Portal stub ===
    print(f"    ⚠ All sources failed, using portal stub")
    return [create_portal_stub('ME')]


# =============================================================================
# NEW HAMPSHIRE DOT PARSER
# =============================================================================

def parse_nhdot() -> List[Dict]:
    """
    Parse NHDOT bid information.
    
    Sources (in priority order):
    1. Invitation to Bid HTML page - current active bids with project details
    2. Advertising Schedule PDF - future projects with dates
    3. Portal stub fallback
    
    Note: NHDOT manages ~$200M annual construction program
    """
    lettings = []
    
    # URLs
    itb_url = "https://www.dot.nh.gov/doing-business-nhdot/contractors/invitation-bid"
    alt_itb_url = "https://www.nh.gov/dot/org/administration/finance/bids/invitations/index.htm"
    ad_schedule_url = "https://mm.nh.gov/files/uploads/dot/remote-docs/current-ad-schedule.pdf"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    # === ATTEMPT 1: Parse Invitation to Bid HTML ===
    for url in [itb_url, alt_itb_url]:
        try:
            print(f"    🔍 Fetching NHDOT Invitation to Bid...")
            response = requests.get(url, timeout=30, headers=headers)
            
            if response.status_code == 403:
                print(f"    ⚠ Access blocked (403), trying alternative...")
                continue
                
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            print(f"    📄 Got HTML: {len(response.text)} bytes")
            
            # Look for project entries - NHDOT uses various formats
            # Try to find project blocks with project numbers like 45075, 43071A, etc.
            text = soup.get_text()
            
            # Pattern for NHDOT projects: Project number followed by scope/description
            # Examples from search results:
            # - "Statewide Tier 2 Resurfacing (Central) 45075"
            # - "Belmont-Gilford-Tilton 45073"
            # - "Manchester-Hooksett 43071A"
            
            # Find all project number patterns
            project_patterns = [
                r'([A-Za-z\-\s]+)\s+(\d{5}[A-Z]?)\s+[Ss]cope[:\s]+([^\n]+)',
                r'(\d{5}[A-Z]?)\s*[-–]\s*([^\n]+)',
                r'Project\s+(?:Number|#|No\.?)?\s*[:\s]*(\d{5}[A-Z]?)',
            ]
            
            found_projects = []
            
            for pattern in project_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        found_projects.append(match)
                    else:
                        found_projects.append((match,))
            
            # Also look for table rows with project info
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        row_text = ' '.join(c.get_text(strip=True) for c in cells)
                        # Look for project numbers
                        proj_match = re.search(r'(\d{5}[A-Z]?)', row_text)
                        if proj_match:
                            found_projects.append((row_text, proj_match.group(1)))
            
            # Also look for links to specific projects
            links = soup.find_all('a', href=True)
            for link in links:
                link_text = link.get_text(strip=True)
                href = link.get('href', '')
                
                # NHDOT project links often contain project numbers
                proj_match = re.search(r'(\d{5}[A-Z]?)', f"{link_text} {href}")
                if proj_match and len(link_text) > 10:
                    found_projects.append((link_text, proj_match.group(1), href))
            
            print(f"    🔬 Found {len(found_projects)} potential project entries")
            
            # Process found projects
            seen_ids = set()
            for proj in found_projects:
                try:
                    if len(proj) >= 2:
                        description = str(proj[0])[:200] if proj[0] else ''
                        project_id = str(proj[1]) if len(proj) > 1 else None
                    else:
                        description = str(proj[0])[:200]
                        project_id = re.search(r'(\d{5}[A-Z]?)', description)
                        project_id = project_id.group(1) if project_id else None
                    
                    if not project_id or project_id in seen_ids:
                        continue
                    seen_ids.add(project_id)
                    
                    # Get URL if available
                    proj_url = itb_url
                    if len(proj) >= 3 and proj[2]:
                        if proj[2].startswith('http'):
                            proj_url = proj[2]
                        elif proj[2].startswith('/'):
                            proj_url = f"https://www.dot.nh.gov{proj[2]}"
                    
                    # Determine project type from description
                    desc_lower = description.lower()
                    if 'resurfacing' in desc_lower or 'paving' in desc_lower or 'pavement' in desc_lower:
                        proj_type = 'Pavement'
                    elif 'bridge' in desc_lower:
                        proj_type = 'Bridge'
                    elif 'i-93' in desc_lower or 'i-89' in desc_lower or 'interstate' in desc_lower:
                        proj_type = 'Highway'
                    elif 'intersection' in desc_lower or 'signal' in desc_lower:
                        proj_type = 'Safety'
                    else:
                        proj_type = 'Highway'
                    
                    # Extract location from description
                    location = None
                    # Common NH town names in the beginning of descriptions
                    loc_match = re.match(r'^([A-Z][a-z]+(?:[\-\s][A-Z][a-z]+)*)', description)
                    if loc_match:
                        location = loc_match.group(1)
                    
                    lettings.append({
                        'id': generate_id(f"NH-{project_id}"),
                        'state': 'NH',
                        'project_id': project_id,
                        'description': description,
                        'cost_low': None,  # NHDOT ITB page doesn't show cost estimates
                        'cost_high': None,
                        'cost_display': 'See Bid Docs',
                        'ad_date': None,
                        'let_date': None,
                        'project_type': proj_type,
                        'location': location,
                        'district': None,
                        'url': proj_url,
                        'source': 'NHDOT',
                        'business_lines': get_business_lines(description)
                    })
                    
                except Exception as e:
                    continue
            
            if lettings:
                print(f"    ✓ {len(lettings)} projects from Invitation to Bid")
                return lettings
            else:
                print(f"    ⚠ No projects extracted from HTML")
                
        except requests.exceptions.HTTPError as e:
            if '403' in str(e):
                print(f"    ⚠ Access blocked (403)")
            else:
                print(f"    ⚠ HTTP error: {e}")
        except Exception as e:
            print(f"    ⚠ HTML parse error: {e}")
    
    # === ATTEMPT 2: Parse Advertising Schedule PDF ===
    try:
        print(f"    🔄 Fetching NHDOT Advertising Schedule PDF...")
        response = requests.get(ad_schedule_url, timeout=60, headers=headers)
        
        if response.status_code == 403:
            print(f"    ⚠ PDF access blocked (403)")
        else:
            response.raise_for_status()
            print(f"    📄 Got PDF: {len(response.content)} bytes")
            
            try:
                import pdfplumber
                import io
                
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    print(f"    📑 PDF has {len(pdf.pages)} pages")
                    
                    for page in pdf.pages:
                        text = page.extract_text() or ''
                        lines = text.split('\n')
                        
                        for line in lines:
                            # Look for project entries with dates and project numbers
                            # Format: "TOWN PROJECT# DATE DESCRIPTION"
                            proj_match = re.search(r'(\d{5}[A-Z]?)', line)
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/20\d{2})', line)
                            
                            if proj_match:
                                project_id = proj_match.group(1)
                                if project_id in [l['project_id'] for l in lettings]:
                                    continue
                                
                                # Parse the line for more details
                                description = line[:200].strip()
                                let_date = None
                                if date_match:
                                    try:
                                        let_date = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                                    except:
                                        pass
                                
                                # Determine project type
                                desc_lower = description.lower()
                                if 'resurfacing' in desc_lower or 'paving' in desc_lower:
                                    proj_type = 'Pavement'
                                elif 'bridge' in desc_lower:
                                    proj_type = 'Bridge'
                                else:
                                    proj_type = 'Highway'
                                
                                lettings.append({
                                    'id': generate_id(f"NH-{project_id}"),
                                    'state': 'NH',
                                    'project_id': project_id,
                                    'description': description,
                                    'cost_low': None,
                                    'cost_high': None,
                                    'cost_display': 'See Bid Docs',
                                    'ad_date': let_date,
                                    'let_date': let_date,
                                    'project_type': proj_type,
                                    'location': None,
                                    'district': None,
                                    'url': itb_url,
                                    'source': 'NHDOT Ad Schedule',
                                    'business_lines': get_business_lines(description)
                                })
                    
                    if lettings:
                        print(f"    ✓ {len(lettings)} projects from Ad Schedule PDF")
                        return lettings
                        
            except ImportError:
                print(f"    ⚠ pdfplumber not installed")
            except Exception as e:
                print(f"    ⚠ PDF parse error: {e}")
                
    except Exception as e:
        print(f"    ⚠ PDF fetch error: {e}")
    
    # === FALLBACK: Portal stub ===
    print(f"    ⚠ All sources failed, using portal stub")
    return [create_portal_stub('NH')]


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
            feed = feedparser.parse(cfg['url'], request_headers={'User-Agent': 'NECMIS/2.0'})
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
    print("NECMIS SCRAPER - PHASE 2.1 (Plain Text Parser)")
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
    print(f"Opportunities: {summary['total_opportunities']}")
    print(f"DOT Lettings: {summary['by_category']['dot_letting']} ({with_cost} with costs, {with_details} with details)")
    print(f"News: {summary['by_category']['news']}")
    print(f"Funding: {summary['by_category']['funding']}")
    print("=" * 60)
    
    return data


if __name__ == '__main__':
    data = run_scraper()
    os.makedirs('data', exist_ok=True)
    with open('data/necmis_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("✓ Saved to data/necmis_data.json")
