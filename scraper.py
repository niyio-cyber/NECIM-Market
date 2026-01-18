#!/usr/bin/env python3
"""
NECMIS Scraper v5 - With Deduplication
======================================
MA: Plain text parser (working - 25 projects, $209M)
ME: Excel parser for CAP Schedule (working)
NH: RPC TIP PDFs with DEDUPLICATION + manual STIP workflow

Key Improvements:
- Deduplication by project_id across all sources
- Manual PDF commit workflow for blocked NH STIP
- Preserved working MA and ME parsers
"""

import json
import hashlib
import re
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

try:
    import requests
    import feedparser
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Missing dependency: {e}")
    raise

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed - Maine Excel parser will use fallback")

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Note: pdfplumber not installed - NH RPC PDF parsing unavailable")


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
    'Providence Journal': {'url': 'https://www.providencejournal.com/arcio/rss/category/news/local/', 'state': 'RI'},
    'Hartford Courant': {'url': 'https://www.courant.com/arcio/rss/category/news/connecticut/', 'state': 'CT'},
}

CONSTRUCTION_KEYWORDS = [
    'construction', 'highway', 'road', 'bridge', 'paving', 'asphalt',
    'concrete', 'infrastructure', 'transportation', 'DOT', 'bid',
    'contract', 'project', 'million', 'grant', 'federal', 'funding',
    'resurfacing', 'rehabilitation', 'maintenance', 'repair',
    'aggregate', 'gravel', 'sand', 'quarry', 'crusher', 'hot mix',
    'ready mix', 'bitumen', 'trucking', 'hauling', 'excavation'
]

# NH RPC TIP PDF sources (accessible, unlike blocked NHDOT)
NH_RPC_TIP_PDFS = {
    'Rockingham_Full': 'https://www.therpc.org/wp-content/uploads/2024/09/RPC-FY-2025-2028-TIP-Amendment-1-Full.pdf',
    'Rockingham_Air': 'https://www.therpc.org/wp-content/uploads/2024/09/RPC-FY-2025-2028-TIP-Amendment-1-Air-Quality.pdf',
}

# Manual STIP PDFs (user commits these to data/nh_stip/ directory)
MANUAL_NH_STIP_DIR = 'data/nh_stip'


def format_currency(amount: float) -> str:
    if amount >= 1_000_000_000:
        return f"${amount/1_000_000_000:.1f}B"
    elif amount >= 1_000_000:
        return f"${amount/1_000_000:.1f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:.0f}K"
    return f"${amount:.0f}"


def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:12]


# =============================================================================
# DEDUPLICATION
# =============================================================================

class ProjectDeduplicator:
    """Deduplicates projects by project_id across all sources."""
    
    def __init__(self):
        self.seen_ids: Set[str] = set()
        self.seen_titles: Set[str] = set()
        self.duplicates_removed = 0
    
    def normalize_project_id(self, project_id: str) -> str:
        """Normalize project ID for comparison."""
        if not project_id:
            return ""
        # Remove common prefixes/suffixes, standardize format
        normalized = re.sub(r'[^A-Z0-9]', '', project_id.upper())
        return normalized
    
    def normalize_title(self, title: str) -> str:
        """Normalize title for fuzzy matching."""
        if not title:
            return ""
        # Lowercase, remove punctuation, normalize whitespace
        normalized = re.sub(r'[^\w\s]', '', title.lower())
        normalized = ' '.join(normalized.split())
        return normalized
    
    def is_duplicate(self, project: Dict) -> bool:
        """Check if project is a duplicate."""
        # Check by project_id first (most reliable)
        project_id = project.get('project_id', '')
        if project_id:
            normalized_id = self.normalize_project_id(project_id)
            if normalized_id and normalized_id in self.seen_ids:
                self.duplicates_removed += 1
                return True
            if normalized_id:
                self.seen_ids.add(normalized_id)
        
        # Fallback: check by normalized title
        title = project.get('title', '')
        if title:
            normalized_title = self.normalize_title(title)
            if normalized_title and normalized_title in self.seen_titles:
                self.duplicates_removed += 1
                return True
            if normalized_title:
                self.seen_titles.add(normalized_title)
        
        return False
    
    def deduplicate(self, projects: List[Dict]) -> List[Dict]:
        """Remove duplicates from project list."""
        unique = []
        for p in projects:
            if not self.is_duplicate(p):
                unique.append(p)
        return unique


# =============================================================================
# MASSDOT PARSER (PRESERVED - WORKING)
# =============================================================================

def parse_massdot() -> List[Dict]:
    """Parse MassDOT advertised projects - plain text parser (WORKING)."""
    url = 'https://hwy.massdot.state.ma.us/webapps/const/statusReport.asp'
    print(f"    🔍 Fetching MassDOT...")
    
    try:
        resp = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NECMIS/1.0)'
        })
        resp.raise_for_status()
        print(f"    📄 Got {len(resp.content)} bytes")
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        text = soup.get_text(separator=' | ')
        text = re.sub(r'\s+', ' ', text)
        print(f"    📝 Converted to {len(text)} chars of text")
        print(f"    🔬 Sample: {text[:300]}...")
        
        projects = []
        
        # Split by "Location:" which starts each project block
        blocks = re.split(r'(?=Location:\s*[A-Z])', text)
        blocks = [b for b in blocks if b.strip() and 'Location:' in b]
        print(f"    📦 Found {len(blocks)} potential project blocks")
        
        for block in blocks:
            # Extract location
            loc_match = re.search(r'Location:\s*([A-Z][A-Z0-9\-]+)', block)
            if not loc_match:
                continue
            location = loc_match.group(1)
            
            # Extract description
            desc_match = re.search(r'Description:\s*(.+?)(?:Project\s*#|Low\s*Bid|$)', block, re.DOTALL)
            description = desc_match.group(1).strip() if desc_match else ""
            description = re.sub(r'\s*\|\s*', ' ', description)[:200]
            
            # Extract project number
            proj_match = re.search(r'Project\s*#[:\s]*([0-9]+)', block)
            project_id = proj_match.group(1) if proj_match else None
            
            # Extract dollar amount - look for patterns like "$1,234,567" or "1,234,567.00"
            money_match = re.search(r'\$?\s*([\d,]+(?:\.\d{2})?)\s*(?:Low|Bid|Total)?', block)
            cost = None
            if money_match:
                try:
                    cost_str = money_match.group(1).replace(',', '')
                    cost = float(cost_str)
                    if cost < 10000:  # Too small, probably not a project cost
                        cost = None
                except:
                    pass
            
            # Extract bid date
            date_match = re.search(r'Bid Opening[:\s]*(\d{1,2}/\d{1,2}/\d{4})', block)
            bid_date = date_match.group(1) if date_match else None
            
            # Determine project type from description
            desc_lower = description.lower()
            if any(x in desc_lower for x in ['bridge', 'culvert', 'structure']):
                project_type = 'Bridge'
            elif any(x in desc_lower for x in ['signal', 'traffic', 'intersection']):
                project_type = 'Traffic'
            elif any(x in desc_lower for x in ['resurface', 'overlay', 'pavement', 'paving']):
                project_type = 'Resurfacing'
            elif any(x in desc_lower for x in ['highway', 'road', 'route']):
                project_type = 'Highway'
            else:
                project_type = 'Construction'
            
            if description and cost and cost >= 10000:
                projects.append({
                    'id': generate_id(f"MA-{project_id or location}-{description[:50]}"),
                    'state': 'MA',
                    'title': f"{location}: {description[:100]}",
                    'description': description,
                    'location': location,
                    'project_id': project_id,
                    'project_type': project_type,
                    'cost_low': cost,
                    'cost_high': cost,
                    'bid_date': bid_date,
                    'url': url,
                    'source': 'MassDOT',
                    'category': 'dot_letting',
                    'fetched': datetime.now(timezone.utc).isoformat()
                })
        
        print(f"    📊 Extracted {len(projects)} projects with values")
        total = sum(p['cost_low'] for p in projects)
        print(f"    ✓ {len(projects)} projects, {format_currency(total)} total pipeline")
        return projects
        
    except Exception as e:
        print(f"    ⚠ MassDOT error: {e}")
        return []


# =============================================================================
# MAINEDOT PARSER (PRESERVED - WORKING)
# =============================================================================

def parse_mainedot() -> List[Dict]:
    """Parse MaineDOT CAP Schedule - Excel parser (WORKING)."""
    excel_url = 'https://www.maine.gov/mdot/projects/workplan/docs/cap-schedules.xlsx'
    print(f"    🔍 Fetching MaineDOT Excel...")
    
    if not PANDAS_AVAILABLE:
        print("    ⚠ pandas not available, using fallback")
        return parse_mainedot_html_fallback()
    
    try:
        resp = requests.get(excel_url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NECMIS/1.0)'
        })
        resp.raise_for_status()
        print(f"    📄 Got {len(resp.content)} bytes")
        
        # Save to temp file and parse
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            f.write(resp.content)
            temp_path = f.name
        
        try:
            df = pd.read_excel(temp_path, sheet_name=0)
            print(f"    📊 Excel has {len(df)} rows, {len(df.columns)} columns")
            print(f"    📋 Columns: {list(df.columns)[:10]}...")
            
            projects = []
            
            # Find relevant columns (MaineDOT format varies)
            col_map = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'win' in col_lower or 'project' in col_lower and 'id' in col_lower:
                    col_map['project_id'] = col
                elif 'location' in col_lower or 'town' in col_lower:
                    col_map['location'] = col
                elif 'description' in col_lower or 'scope' in col_lower:
                    col_map['description'] = col
                elif 'estimate' in col_lower or 'cost' in col_lower or 'amount' in col_lower:
                    col_map['cost'] = col
                elif 'let' in col_lower or 'advertise' in col_lower or 'bid' in col_lower:
                    col_map['date'] = col
            
            print(f"    🗂️ Column mapping: {col_map}")
            
            for idx, row in df.iterrows():
                try:
                    project_id = str(row.get(col_map.get('project_id', ''), '')).strip()
                    location = str(row.get(col_map.get('location', ''), '')).strip()
                    description = str(row.get(col_map.get('description', ''), '')).strip()
                    
                    # Parse cost
                    cost = None
                    cost_val = row.get(col_map.get('cost', ''), '')
                    if pd.notna(cost_val):
                        try:
                            cost_str = str(cost_val).replace('$', '').replace(',', '').strip()
                            cost = float(cost_str)
                            if cost < 10000:
                                cost = None
                        except:
                            pass
                    
                    # Parse date
                    bid_date = None
                    date_val = row.get(col_map.get('date', ''), '')
                    if pd.notna(date_val):
                        try:
                            if hasattr(date_val, 'strftime'):
                                bid_date = date_val.strftime('%Y-%m-%d')
                            else:
                                bid_date = str(date_val)[:10]
                        except:
                            pass
                    
                    if location and (description or project_id):
                        title = f"{location}: {description[:100]}" if description else f"{location}: Project {project_id}"
                        
                        projects.append({
                            'id': generate_id(f"ME-{project_id or location}-{description[:30]}"),
                            'state': 'ME',
                            'title': title,
                            'description': description or f"MaineDOT Project {project_id}",
                            'location': location,
                            'project_id': project_id if project_id and project_id != 'nan' else None,
                            'project_type': 'Highway',
                            'cost_low': cost,
                            'cost_high': cost,
                            'bid_date': bid_date,
                            'url': 'https://www.maine.gov/mdot/projects/workplan/',
                            'source': 'MaineDOT',
                            'category': 'dot_letting',
                            'fetched': datetime.now(timezone.utc).isoformat()
                        })
                except Exception as row_e:
                    continue
            
            with_cost = len([p for p in projects if p.get('cost_low')])
            total = sum(p.get('cost_low') or 0 for p in projects)
            print(f"    ✓ {len(projects)} projects ({with_cost} with costs), {format_currency(total)} pipeline")
            return projects
            
        finally:
            os.unlink(temp_path)
            
    except Exception as e:
        print(f"    ⚠ Excel failed: {e}")
        return parse_mainedot_html_fallback()


def parse_mainedot_html_fallback() -> List[Dict]:
    """Fallback HTML parser for MaineDOT."""
    print("    🔄 Trying HTML fallback...")
    try:
        url = 'https://www.maine.gov/mdot/projects/advertised/'
        resp = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NECMIS/1.0)'
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        projects = []
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    # Try to extract project info from cells
                    text = ' '.join(c.get_text(strip=True) for c in cells)
                    if any(kw in text.lower() for kw in ['highway', 'bridge', 'road', 'route']):
                        projects.append({
                            'id': generate_id(f"ME-{text[:50]}"),
                            'state': 'ME',
                            'title': text[:150],
                            'description': text,
                            'url': url,
                            'source': 'MaineDOT',
                            'category': 'dot_letting',
                            'fetched': datetime.now(timezone.utc).isoformat()
                        })
        
        print(f"    ✓ HTML fallback: {len(projects)} projects")
        return projects
        
    except Exception as e:
        print(f"    ⚠ HTML fallback failed: {e}")
        return [{
            'id': generate_id('maine-portal'),
            'state': 'ME',
            'title': 'MaineDOT Bid Opportunities',
            'description': 'Current MaineDOT advertised projects and bid opportunities',
            'url': 'https://www.maine.gov/mdot/projects/workplan/',
            'source': 'MaineDOT',
            'category': 'dot_letting',
            'fetched': datetime.now(timezone.utc).isoformat()
        }]


# =============================================================================
# NHDOT PARSER - RPC TIP PDFs + MANUAL STIP
# =============================================================================

def parse_nhdot() -> List[Dict]:
    """Parse NH DOT projects from RPC TIP PDFs + manual STIP."""
    all_projects = []
    
    # 1. Parse RPC TIP PDFs (accessible)
    if PDF_AVAILABLE:
        for name, url in NH_RPC_TIP_PDFS.items():
            print(f"    📄 Fetching {name}...")
            projects = parse_rpc_tip_pdf(url, name)
            all_projects.extend(projects)
    else:
        print("    ⚠ pdfplumber not available - skipping RPC PDFs")
    
    # 2. Parse manually committed STIP PDFs
    manual_projects = parse_manual_nh_stip()
    all_projects.extend(manual_projects)
    
    # 3. Deduplicate NH projects
    dedup = ProjectDeduplicator()
    unique_projects = dedup.deduplicate(all_projects)
    
    if dedup.duplicates_removed > 0:
        print(f"    🔄 Deduplication: removed {dedup.duplicates_removed} duplicates")
    
    with_cost = len([p for p in unique_projects if p.get('cost_low')])
    total = sum(p.get('cost_low') or 0 for p in unique_projects)
    print(f"    ✓ NH Total: {len(unique_projects)} unique projects ({with_cost} with costs), {format_currency(total)}")
    
    # If no projects found, return portal stub
    if not unique_projects:
        return [{
            'id': generate_id('nh-portal'),
            'state': 'NH',
            'title': 'NHDOT Bid Opportunities',
            'description': 'Current NHDOT advertised projects',
            'url': 'https://www.dot.nh.gov/doing-business/contractors',
            'source': 'NHDOT',
            'category': 'dot_letting',
            'fetched': datetime.now(timezone.utc).isoformat()
        }]
    
    return unique_projects


def parse_rpc_tip_pdf(url: str, source_name: str) -> List[Dict]:
    """Parse an RPC TIP PDF for project data."""
    try:
        import tempfile
        
        resp = requests.get(url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; NECMIS/1.0)'
        })
        resp.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
            f.write(resp.content)
            temp_path = f.name
        
        try:
            projects = []
            with pdfplumber.open(temp_path) as pdf:
                print(f"      📑 {len(pdf.pages)} pages")
                
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if not row or len(row) < 3:
                                continue
                            
                            # Look for project ID patterns (e.g., "12345", "X-A000(123)")
                            project_id = None
                            location = None
                            description = None
                            cost = None
                            
                            for cell in row:
                                if not cell:
                                    continue
                                cell_str = str(cell).strip()
                                
                                # Project ID patterns
                                if re.match(r'^\d{4,6}[A-Z]?$', cell_str) or \
                                   re.match(r'^[A-Z]-[A-Z]\d{3}\(\d+\)', cell_str):
                                    project_id = cell_str
                                
                                # Cost patterns ($X,XXX,XXX or X,XXX,XXX)
                                cost_match = re.search(r'\$?([\d,]+(?:\.\d{2})?)', cell_str)
                                if cost_match:
                                    try:
                                        val = float(cost_match.group(1).replace(',', ''))
                                        if val >= 100000:  # At least $100K
                                            cost = val
                                    except:
                                        pass
                                
                                # Location patterns (Town names, Routes)
                                if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$', cell_str) and len(cell_str) < 30:
                                    location = cell_str
                                elif 'Route' in cell_str or 'I-' in cell_str or 'NH ' in cell_str:
                                    if not description:
                                        description = cell_str
                                
                                # Description (longer text)
                                if len(cell_str) > 30 and not description:
                                    description = cell_str[:200]
                            
                            if project_id and (location or description):
                                projects.append({
                                    'id': generate_id(f"NH-{project_id}-{source_name}"),
                                    'state': 'NH',
                                    'title': f"{location or 'NH'}: {description or f'Project {project_id}'}",
                                    'description': description or f"NHDOT Project {project_id}",
                                    'location': location,
                                    'project_id': project_id,
                                    'project_type': 'Highway',
                                    'cost_low': cost,
                                    'cost_high': cost,
                                    'url': url,
                                    'source': f'NH RPC ({source_name})',
                                    'category': 'dot_letting',
                                    'fetched': datetime.now(timezone.utc).isoformat()
                                })
            
            with_cost = len([p for p in projects if p.get('cost_low')])
            total = sum(p.get('cost_low') or 0 for p in projects)
            print(f"      ✓ {len(projects)} projects ({with_cost} with costs), {format_currency(total)}")
            return projects
            
        finally:
            os.unlink(temp_path)
            
    except Exception as e:
        print(f"      ⚠ RPC PDF error: {e}")
        return []


def parse_manual_nh_stip() -> List[Dict]:
    """Parse manually committed NH STIP PDFs from data/nh_stip/ directory."""
    if not os.path.exists(MANUAL_NH_STIP_DIR):
        return []
    
    if not PDF_AVAILABLE:
        return []
    
    projects = []
    pdf_files = [f for f in os.listdir(MANUAL_NH_STIP_DIR) if f.endswith('.pdf')]
    
    if pdf_files:
        print(f"    📁 Found {len(pdf_files)} manual STIP PDFs")
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(MANUAL_NH_STIP_DIR, pdf_file)
        print(f"      📄 Parsing {pdf_file}...")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if not row or len(row) < 3:
                                continue
                            
                            project_id = None
                            location = None
                            description = None
                            cost = None
                            
                            for cell in row:
                                if not cell:
                                    continue
                                cell_str = str(cell).strip()
                                
                                # Project ID
                                if re.match(r'^\d{4,6}[A-Z]?$', cell_str):
                                    project_id = cell_str
                                
                                # Cost
                                cost_match = re.search(r'\$?([\d,]+(?:\.\d{2})?)', cell_str)
                                if cost_match:
                                    try:
                                        val = float(cost_match.group(1).replace(',', ''))
                                        if val >= 100000:
                                            cost = val
                                    except:
                                        pass
                                
                                # Location
                                if re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$', cell_str) and len(cell_str) < 30:
                                    location = cell_str
                                
                                # Description
                                if len(cell_str) > 30 and not description:
                                    description = cell_str[:200]
                            
                            if project_id and (location or description):
                                projects.append({
                                    'id': generate_id(f"NH-STIP-{project_id}"),
                                    'state': 'NH',
                                    'title': f"{location or 'NH'}: {description or f'Project {project_id}'}",
                                    'description': description or f"NHDOT STIP Project {project_id}",
                                    'location': location,
                                    'project_id': project_id,
                                    'project_type': 'Highway',
                                    'cost_low': cost,
                                    'cost_high': cost,
                                    'url': 'https://www.dot.nh.gov/projects-plans-and-programs/stip',
                                    'source': f'NH STIP (Manual: {pdf_file})',
                                    'category': 'dot_letting',
                                    'fetched': datetime.now(timezone.utc).isoformat()
                                })
        except Exception as e:
            print(f"      ⚠ Error parsing {pdf_file}: {e}")
    
    if projects:
        with_cost = len([p for p in projects if p.get('cost_low')])
        total = sum(p.get('cost_low') or 0 for p in projects)
        print(f"      ✓ Manual STIP: {len(projects)} projects ({with_cost} with costs), {format_currency(total)}")
    
    return projects


# =============================================================================
# OTHER STATE STUBS
# =============================================================================

def get_portal_stubs() -> List[Dict]:
    """Return portal links for states without active parsing."""
    portals = [
        ('VT', 'VTrans Bid Results', 'https://vtrans.vermont.gov/contract-admin/bids-requests'),
        ('NY', 'NYSDOT Contract Letting', 'https://www.dot.ny.gov/doing-business/opportunities/const-letting'),
        ('RI', 'RIDOT Projects', 'https://www.dot.ri.gov/projects/'),
        ('CT', 'CTDOT Contract Letting', 'https://portal.ct.gov/dot/business/construction/contract-letting-schedule'),
        ('PA', 'PennDOT Lettings', 'https://www.penndot.pa.gov/ProjectAndPrograms/Construction/Pages/Letting-Info.aspx'),
    ]
    
    return [{
        'id': generate_id(f"{state}-portal"),
        'state': state,
        'title': title,
        'description': f'Current {state} DOT advertised projects',
        'url': url,
        'source': f'{state}DOT',
        'category': 'dot_letting',
        'fetched': datetime.now(timezone.utc).isoformat()
    } for state, title, url in portals]


# =============================================================================
# RSS FEEDS
# =============================================================================

def fetch_rss_feeds() -> List[Dict]:
    """Fetch and filter RSS feeds."""
    all_news = []
    
    for name, config in RSS_FEEDS.items():
        print(f"  📰 {name}...")
        try:
            feed = feedparser.parse(config['url'])
            items = []
            
            for entry in feed.entries[:20]:
                title = entry.get('title', '')
                summary = entry.get('summary', entry.get('description', ''))
                text = f"{title} {summary}".lower()
                
                if any(kw in text for kw in CONSTRUCTION_KEYWORDS):
                    items.append({
                        'id': generate_id(entry.get('link', title)),
                        'state': config['state'],
                        'title': title,
                        'description': summary[:300] if summary else '',
                        'url': entry.get('link', ''),
                        'source': name,
                        'category': 'news' if 'grant' not in text and 'funding' not in text else 'funding',
                        'published': entry.get('published', ''),
                        'fetched': datetime.now(timezone.utc).isoformat()
                    })
            
            all_news.extend(items)
            print(f"    ✓ {len(items)} items")
            
        except Exception as e:
            print(f"    ⚠ {name} error: {e}")
    
    return all_news


# =============================================================================
# MARKET HEALTH
# =============================================================================

def calculate_market_health(dot_lettings: List[Dict], news: List[Dict]) -> Dict:
    """Calculate market health scores."""
    # DOT Pipeline Score (0-10)
    total_value = sum(d.get('cost_low') or 0 for d in dot_lettings)
    projects_with_cost = len([d for d in dot_lettings if d.get('cost_low')])
    
    if total_value >= 500_000_000:
        dot_score = 10
    elif total_value >= 200_000_000:
        dot_score = 9
    elif total_value >= 100_000_000:
        dot_score = 8
    elif total_value >= 50_000_000:
        dot_score = 7
    elif total_value >= 20_000_000:
        dot_score = 6
    else:
        dot_score = max(3, projects_with_cost // 2)
    
    # News Score
    construction_news = len([n for n in news if n['category'] == 'news'])
    funding_news = len([n for n in news if n['category'] == 'funding'])
    news_score = min(10, 5 + construction_news // 2 + funding_news)
    
    # Overall Score
    overall = round((dot_score * 0.7 + news_score * 0.3), 1)
    
    if overall >= 8:
        status = 'strong'
    elif overall >= 6:
        status = 'stable'
    else:
        status = 'cautious'
    
    return {
        'overall_score': overall,
        'overall_status': status,
        'dot_pipeline': {
            'score': dot_score,
            'total_value': total_value,
            'projects_with_cost': projects_with_cost
        },
        'news_activity': {
            'score': news_score,
            'construction_news': construction_news,
            'funding_news': funding_news
        }
    }


# =============================================================================
# MAIN
# =============================================================================

def fetch_dot_lettings() -> List[Dict]:
    """Fetch all DOT lettings with deduplication."""
    all_lettings = []
    
    # MassDOT (working)
    print(f"  🏗️ MassDOT (MA)...")
    ma_projects = parse_massdot()
    all_lettings.extend(ma_projects)
    
    # MaineDOT (working)
    print(f"  🏗️ MaineDOT (ME)...")
    me_projects = parse_mainedot()
    all_lettings.extend(me_projects)
    
    # NHDOT (RPC PDFs + manual STIP with deduplication)
    print(f"  🏗️ NHDOT (NH)...")
    nh_projects = parse_nhdot()
    all_lettings.extend(nh_projects)
    
    # Other states (portal stubs)
    for state in ['VT', 'NY', 'RI', 'CT', 'PA']:
        print(f"  🏗️ {state}DOT ({state})...")
        print(f"    ✓ Portal link")
    all_lettings.extend(get_portal_stubs())
    
    return all_lettings


def build_summary(dot_lettings: List[Dict], news: List[Dict]) -> Dict:
    """Build summary statistics."""
    all_items = dot_lettings + news
    
    by_state = {}
    for state in STATES:
        state_items = [i for i in all_items if i.get('state') == state]
        state_value = sum(i.get('cost_low') or 0 for i in state_items)
        by_state[state] = {
            'count': len(state_items),
            'total_value': state_value
        }
    
    by_cat = {}
    for cat in ['dot_letting', 'news', 'funding']:
        by_cat[cat] = len([i for i in all_items if i.get('category') == cat])
    
    total_low = sum(i.get('cost_low') or 0 for i in all_items)
    total_high = sum(i.get('cost_high') or i.get('cost_low') or 0 for i in all_items)
    
    return {
        'total_opportunities': len(all_items),
        'total_value_low': total_low,
        'total_value_high': total_high,
        'by_state': by_state,
        'by_category': by_cat
    }


def run_scraper() -> Dict:
    """Main scraper entry point."""
    print("=" * 60)
    print("NECMIS SCRAPER v5 - WITH DEDUPLICATION")
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
        state_with_cost = len([d for d in state_projects if d.get('cost_low')])
        print(f"  {state}: {len(state_projects)} projects ({state_with_cost} with $), {format_currency(state_value)}")
    
    print("=" * 60)
    
    return data


if __name__ == '__main__':
    data = run_scraper()
    os.makedirs('data', exist_ok=True)
    with open('data/necmis_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("✓ Saved to data/necmis_data.json")
