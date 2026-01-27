#!/usr/bin/env python3
"""
NECMIS Market Health Engine v2.0
================================
Real-time market health scoring using external APIs (FRED, EIA, Census).

v2.0 Changes:
- DOT Pipeline: Time-weighted scoring (near-term projects count more)
- DOT Pipeline: FHWA-weighted state extrapolation (not naive linear)
- DOT Pipeline: Accepts project-level data for proper scoring
- Input Cost: Combined gas/diesel (60/40 weighting)

Usage:
    from market_health_engine import calculate_market_health
    
    # With DOT pipeline project data from scraper
    mh = calculate_market_health(dot_projects=scraper_output['dot_lettings'])
    
    # Or with just total (legacy mode - less accurate)
    mh = calculate_market_health(dot_pipeline_total=150_000_000)
"""

import os
import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

try:
    import requests
except ImportError:
    print("Missing requests library. Install with: pip install requests")
    raise


# =============================================================================
# FHWA APPORTIONMENT RATIOS (FY2024) - For state-weighted extrapolation
# =============================================================================

FHWA_APPORTIONMENTS = {
    'NY': 1_829_000_000,   # $1.829B - 31.4%
    'PA': 1_901_000_000,   # $1.901B - 32.6%
    'MA': 719_000_000,     # $719M - 12.3%
    'CT': 558_000_000,     # $558M - 9.6%
    'NH': 193_000_000,     # $193M - 3.3%
    'ME': 213_000_000,     # $213M - 3.7%
    'VT': 200_000_000,     # $200M - 3.4%
    'RI': 216_000_000,     # $216M - 3.7%
}

# Calculate 8-state total and ratios
FHWA_8_STATE_TOTAL = sum(FHWA_APPORTIONMENTS.values())  # $5.829B

STATE_RATIOS = {
    state: amount / FHWA_8_STATE_TOTAL 
    for state, amount in FHWA_APPORTIONMENTS.items()
}


# =============================================================================
# TIME WEIGHTING FOR DOT PIPELINE
# =============================================================================

def get_time_weight(project_date: Optional[str], reference_date: datetime = None) -> float:
    """
    Calculate time weight for a project based on its bid/let date.
    Near-term = full weight, long-term = reduced weight.
    """
    if reference_date is None:
        reference_date = datetime.now()
    
    if not project_date:
        return 0.5  # No date = assume mid-term
    
    try:
        proj_date = datetime.strptime(project_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        return 0.5
    
    days_out = (proj_date - reference_date).days
    
    if days_out < 0:
        return 0.8  # Past date - still valuable
    elif days_out <= 180:  # 0-6 months
        return 1.0
    elif days_out <= 365:  # 6-12 months
        return 0.7
    elif days_out <= 540:  # 12-18 months
        return 0.5
    elif days_out <= 730:  # 18-24 months
        return 0.3
    else:  # 24+ months
        return 0.1


def categorize_time_horizon(project_date: Optional[str], reference_date: datetime = None) -> str:
    """Categorize project into near/mid/long term buckets."""
    if reference_date is None:
        reference_date = datetime.now()
    
    if not project_date:
        return 'unknown'
    
    try:
        proj_date = datetime.strptime(project_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        return 'unknown'
    
    days_out = (proj_date - reference_date).days
    
    if days_out <= 180:
        return 'near'
    elif days_out <= 540:
        return 'mid'
    else:
        return 'long'


# =============================================================================
# CONFIGURATION
# =============================================================================

# API Keys (set via environment variables for security)
FRED_API_KEY = os.environ.get('FRED_API_KEY', '')  # Get free key at https://fred.stlouisfed.org/docs/api/api_key.html
EIA_API_KEY = os.environ.get('EIA_API_KEY', '')    # Get free key at https://www.eia.gov/opendata/register.php

# State FIPS codes for Census API
STATE_FIPS = {
    'MA': '25', 'NH': '33', 'ME': '23', 'CT': '09',
    'VT': '50', 'NY': '36', 'RI': '44', 'PA': '42'
}

# FRED Series IDs
FRED_SERIES = {
    'housing_permits': {
        'MA': 'MABPPRIVSA', 'NH': 'NHBPPRIVSA', 'ME': 'MEBPPRIVSA', 'CT': 'CTBPPRIVSA',
        'VT': 'VTBPPRIVSA', 'NY': 'NYBPPRIVSA', 'RI': 'RIBPPRIVSA', 'PA': 'PABPPRIVSA'
    },
    'construction_employment': {
        'MA': 'MACONSN', 'NH': 'NHCONSN', 'ME': 'MECONSN', 'CT': 'CTCONSN',
        'VT': 'VTCONSN', 'NY': 'NYCONSN', 'RI': 'RICONSN', 'PA': 'PACONSN'
    },
    'construction_spending': 'TLHWYCONS'  # National highway construction
}

# IIJA Funding (hardcoded - legislated through FY2026)
IIJA_FUNDING = {
    'FY2022': 6_200_000_000,
    'FY2023': 6_500_000_000,
    'FY2024': 6_700_000_000,
    'FY2025': 6_800_000_000,
    'FY2026': 7_000_000_000,
}

# Scoring weights
WEIGHTS = {
    'dot_pipeline': 0.15,
    'housing_permits': 0.10,
    'construction_spending': 0.08,
    'migration': 0.07,
    'construction_employment': 0.08,
    'input_cost': 0.07,
    'infrastructure_funding': 0.05,
}

# Baselines (from PRD - verified against 2024 data)
BASELINES = {
    'dot_pipeline': 6_000_000_000,      # $6.0B visible pipeline (v2 - derived from FHWA)
    'housing_permits_monthly': 15_000,   # 15K permits/month across 8 states
    'construction_spending': 143_000,    # $143B SAAR (millions in FRED)
    'gasoline': 3.20,                    # $3.20/gal regular gasoline (2024 avg)
    'diesel': 4.00,                      # $4.00/gal diesel (2024 actual avg)
    'infrastructure_funding': 5_500_000_000,  # $5.5B pre-IIJA
}

# DOT Pipeline scoring parameters (v2)
DOT_SCORE_AT_BASELINE = 7.0  # Score when pipeline equals baseline

# Input cost weights (gas is more important for fleet operations)
INPUT_COST_WEIGHTS = {
    'gasoline': 0.60,  # 60% weight - more vehicles use gas
    'diesel': 0.40,    # 40% weight - heavy equipment
}

# Cache for historical data (persisted to JSON)
CACHE_FILE = Path('data/market_health_cache.json')


# =============================================================================
# API CLIENTS
# =============================================================================

def fetch_fred_series(series_id: str, limit: int = 24) -> List[Dict]:
    """Fetch data from FRED API."""
    if not FRED_API_KEY:
        print(f"  ⚠️  FRED_API_KEY not set, using fallback for {series_id}")
        return []
    
    url = 'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'sort_order': 'desc',
        'limit': limit
    }
    
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        observations = data.get('observations', [])
        # Filter out missing values
        return [{'date': o['date'], 'value': float(o['value'])} 
                for o in observations if o['value'] != '.']
    except Exception as e:
        print(f"  ⚠️  FRED API error for {series_id}: {e}")
        return []


def fetch_eia_fuel_prices(weeks: int = 12) -> Dict[str, List[float]]:
    """Fetch PADD 1A gasoline and diesel prices from EIA API."""
    
    # Product codes for EIA API
    products = {
        'gasoline': 'EMM_EPMR_PTE_R1X_DPG',  # Regular gasoline, PADD 1A
        'diesel': 'EMD_EPD2D_PTE_R1X_DPG',    # No 2 Diesel, PADD 1A
    }
    
    # Fallback data (actual late 2024 / early 2025 prices)
    fallback = {
        'gasoline': [3.15, 3.18, 3.20, 3.22, 3.25, 3.28, 3.30, 3.28, 3.25, 3.22, 3.20, 3.18],
        'diesel': [3.76, 3.76, 3.76, 3.85, 4.03, 4.10, 4.09, 4.21, 4.31, 4.30, 4.33, 4.31]
    }
    
    if not EIA_API_KEY:
        print("  ⚠️  EIA_API_KEY not set, using historical fallback")
        return fallback
    
    result = {}
    
    for fuel_type, product_code in products.items():
        url = 'https://api.eia.gov/v2/petroleum/pri/gnd/data/'
        params = {
            'api_key': EIA_API_KEY,
            'frequency': 'weekly',
            'data[0]': 'value',
            'facets[duoarea][]': 'R1X',  # PADD 1A (New England)
            'facets[product][]': product_code.split('_')[1] if '_' in product_code else 'EPD2D',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'length': weeks
        }
        
        # Adjust product facet based on fuel type
        if fuel_type == 'gasoline':
            params['facets[product][]'] = 'EPMR'  # Regular gasoline
        else:
            params['facets[product][]'] = 'EPD2D'  # No 2 Diesel
        
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            prices = [float(item['value']) for item in data.get('response', {}).get('data', [])]
            if prices:
                result[fuel_type] = prices[::-1]  # Oldest to newest
            else:
                result[fuel_type] = fallback[fuel_type]
        except Exception as e:
            print(f"  ⚠️  EIA API error for {fuel_type}: {e}")
            result[fuel_type] = fallback[fuel_type]
    
    return result


def fetch_eia_diesel_prices(weeks: int = 12) -> List[float]:
    """Legacy function - now calls combined fetch and returns diesel only."""
    prices = fetch_eia_fuel_prices(weeks)
    return prices.get('diesel', [3.76, 3.76, 3.76, 3.85, 4.03, 4.10, 4.09, 4.21, 4.31, 4.30, 4.33, 4.31])


def fetch_census_population() -> Dict[str, Dict]:
    """Fetch population and migration data from Census API."""
    # Census API is free without key for low volume
    fips_list = ','.join(STATE_FIPS.values())
    
    # Try 2023 data first (most recent available)
    for year in ['2023', '2022']:
        url = f'https://api.census.gov/data/{year}/pep/population'
        params = {
            'get': 'NAME,POP,NPOPCHG',
            'for': f'state:{fips_list}'
        }
        
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                # First row is headers: ['NAME', 'POP', 'NPOPCHG', 'state']
                result = {}
                fips_to_state = {v: k for k, v in STATE_FIPS.items()}
                for row in data[1:]:
                    state_fips = row[3]
                    state = fips_to_state.get(state_fips)
                    if state:
                        result[state] = {
                            'population': int(row[1]),
                            'change': int(row[2]) if row[2] else 0
                        }
                return result
        except Exception as e:
            print(f"  ⚠️  Census API error for {year}: {e}")
            continue
    
    # Fallback to estimated values
    print("  ⚠️  Census API failed, using estimated values")
    return {
        'MA': {'population': 7_001_000, 'change': 15_000},
        'NY': {'population': 19_571_000, 'change': -101_000},
        'PA': {'population': 12_972_000, 'change': -17_000},
        'CT': {'population': 3_626_000, 'change': 4_000},
        'NH': {'population': 1_402_000, 'change': 10_000},
        'ME': {'population': 1_395_000, 'change': 11_000},
        'RI': {'population': 1_096_000, 'change': 2_000},
        'VT': {'population': 647_000, 'change': 1_000},
    }


# =============================================================================
# SCORING FUNCTIONS (from PRD Aggregation Formulas)
# =============================================================================

def score_dot_pipeline(total_pipeline_dollars: float) -> Tuple[float, str]:
    """
    Score DOT project pipeline (legacy mode - simple total).
    Use score_dot_pipeline_v2() for time-weighted scoring with project data.
    
    Formula: Score = (pipeline / $6.0B) × 7.0, clamped to 0-10
    """
    baseline = BASELINES['dot_pipeline']
    raw_score = (total_pipeline_dollars / baseline) * DOT_SCORE_AT_BASELINE
    score = max(0, min(10, raw_score))
    
    # Determine action
    if score >= 8.0:
        action = 'Aggressive expansion - strong pipeline'
    elif score >= 7.0:
        action = 'Expand capacity - healthy pipeline'
    elif score >= 5.5:
        action = 'Maintain position - adequate pipeline'
    elif score >= 4.0:
        action = 'Selective bidding - pipeline softening'
    else:
        action = 'Defensive mode - weak pipeline'
    
    return round(score, 1), action


def score_dot_pipeline_v2(projects: List[Dict], reference_date: datetime = None) -> Dict:
    """
    Score DOT pipeline with time-weighting and FHWA state extrapolation (v2).
    
    Args:
        projects: List of project dicts with keys:
            - state: str (MA, ME, NH, CT, VT, NY, RI, PA)
            - cost_low: float (project value in dollars)
            - ad_date or let_date: str (YYYY-MM-DD format, optional)
        reference_date: Date to calculate time weights from (default: today)
    
    Returns:
        Dict with detailed scoring breakdown
    """
    if reference_date is None:
        reference_date = datetime.now()
    
    # Initialize accumulators
    state_raw_totals = {}
    state_weighted_totals = {}
    horizon_totals = {'near': 0, 'mid': 0, 'long': 0, 'unknown': 0}
    horizon_counts = {'near': 0, 'mid': 0, 'long': 0, 'unknown': 0}
    
    total_raw = 0
    total_weighted = 0
    projects_with_cost = 0
    projects_with_date = 0
    
    for proj in projects:
        state = proj.get('state')
        cost = proj.get('cost_low') or 0
        
        # Get best available date
        proj_date = proj.get('let_date') or proj.get('ad_date')
        
        if cost > 0:
            projects_with_cost += 1
            
            # Track raw totals by state
            if state not in state_raw_totals:
                state_raw_totals[state] = 0
                state_weighted_totals[state] = 0
            state_raw_totals[state] += cost
            
            # Calculate time weight
            weight = get_time_weight(proj_date, reference_date)
            weighted_cost = cost * weight
            state_weighted_totals[state] += weighted_cost
            
            total_raw += cost
            total_weighted += weighted_cost
            
            # Categorize by horizon
            horizon = categorize_time_horizon(proj_date, reference_date)
            horizon_totals[horizon] += cost
            horizon_counts[horizon] += 1
            
            if proj_date:
                projects_with_date += 1
    
    # FHWA-weighted extrapolation
    captured_ratio = sum(STATE_RATIOS.get(s, 0) for s in state_raw_totals.keys())
    
    if captured_ratio > 0:
        extrapolated_raw = total_raw / captured_ratio
        extrapolated_weighted = total_weighted / captured_ratio
    else:
        extrapolated_raw = total_raw
        extrapolated_weighted = total_weighted
    
    # Estimate each missing state
    state_estimates = dict(state_raw_totals)
    for state in ['MA', 'ME', 'NH', 'CT', 'VT', 'NY', 'RI', 'PA']:
        if state not in state_estimates:
            state_estimates[state] = extrapolated_raw * STATE_RATIOS.get(state, 0)
    
    # Score based on TIME-WEIGHTED, EXTRAPOLATED total
    # 
    # CRITICAL FIX: Apply time-weighting to baseline for consistent comparison
    # The $6.0B baseline assumes full 18-month visibility, but time-weighting
    # discounts long-term projects. We must discount the baseline the same way.
    #
    # Calculate actual average time weight from the data:
    if total_raw > 0:
        actual_avg_time_weight = total_weighted / total_raw
    else:
        actual_avg_time_weight = 0.5  # Default if no data
    
    # Time-weighted baseline = raw baseline × actual average weight
    # This ensures we compare weighted pipeline to weighted expectation
    time_weighted_baseline = BASELINES['dot_pipeline'] * actual_avg_time_weight
    
    scoring_value = extrapolated_weighted
    raw_score = (scoring_value / time_weighted_baseline) * DOT_SCORE_AT_BASELINE
    score = max(0, min(10, raw_score))
    
    # Determine action
    if score >= 8.0:
        action = 'Aggressive expansion - strong near-term pipeline'
    elif score >= 7.0:
        action = 'Expand capacity - healthy pipeline'
    elif score >= 5.5:
        action = 'Maintain position - adequate pipeline'
    elif score >= 4.0:
        action = 'Selective bidding - pipeline softening'
    else:
        action = 'Defensive mode - weak pipeline'
    
    # Format currency helper
    def fmt(amt):
        if amt >= 1_000_000_000:
            return f"${amt/1_000_000_000:.2f}B"
        elif amt >= 1_000_000:
            return f"${amt/1_000_000:.1f}M"
        return f"${amt:,.0f}"
    
    return {
        'score': round(score, 1),
        'action': action,
        'raw_total': total_raw,
        'weighted_total': total_weighted,
        'extrapolated_total': extrapolated_raw,
        'extrapolated_weighted': extrapolated_weighted,
        'raw_display': fmt(total_raw),
        'weighted_display': fmt(total_weighted),
        'extrapolated_display': fmt(extrapolated_raw),
        'by_horizon': {
            'near': {'value': horizon_totals['near'], 'display': fmt(horizon_totals['near']), 
                    'count': horizon_counts['near'], 'label': '0-6 months'},
            'mid': {'value': horizon_totals['mid'], 'display': fmt(horizon_totals['mid']), 
                   'count': horizon_counts['mid'], 'label': '6-18 months'},
            'long': {'value': horizon_totals['long'], 'display': fmt(horizon_totals['long']), 
                    'count': horizon_counts['long'], 'label': '18+ months'},
            'unknown': {'value': horizon_totals['unknown'], 'display': fmt(horizon_totals['unknown']), 
                       'count': horizon_counts['unknown'], 'label': 'No date'},
        },
        'by_state': {
            state: {
                'raw': state_raw_totals.get(state, 0),
                'weighted': state_weighted_totals.get(state, 0),
                'estimated': state_estimates.get(state, 0),
                'is_scraped': state in state_raw_totals,
                'fhwa_ratio': f"{STATE_RATIOS.get(state, 0)*100:.1f}%"
            }
            for state in ['MA', 'ME', 'NH', 'CT', 'VT', 'NY', 'RI', 'PA']
        },
        'coverage': {
            'states_with_data': len(state_raw_totals),
            'states_total': 8,
            'market_coverage': f"{captured_ratio*100:.1f}%",
            'projects_with_cost': projects_with_cost,
            'projects_with_date': projects_with_date,
            'date_coverage': f"{projects_with_date/projects_with_cost*100:.1f}%" if projects_with_cost > 0 else "0%"
        },
        'scoring_params': {
            'baseline_raw': BASELINES['dot_pipeline'],
            'baseline_raw_display': fmt(BASELINES['dot_pipeline']),
            'avg_time_weight': round(actual_avg_time_weight, 2),
            'baseline_time_weighted': time_weighted_baseline,
            'baseline_time_weighted_display': fmt(time_weighted_baseline),
            'score_at_baseline': DOT_SCORE_AT_BASELINE,
            'scoring_value': scoring_value,
            'scoring_value_display': fmt(scoring_value)
        }
    }


def score_housing_permits(current_total: float, year_ago_total: float) -> Tuple[float, str, float]:
    """
    Score housing permit momentum.
    Formula: Score = 5.0 + (YoY_change × 20), clamped to 0-10
    """
    if year_ago_total <= 0:
        return 5.0, 'Monitor trends', 0.0
    
    yoy_change = (current_total - year_ago_total) / year_ago_total
    raw_score = 5.0 + (yoy_change * 20)
    score = max(0, min(10, raw_score))
    
    if yoy_change > 0.07:
        action = 'Ready-mix expansion opportunity'
    elif yoy_change > 0:
        action = 'Monitor trends'
    elif yoy_change > -0.10:
        action = 'Selective investment'
    else:
        action = 'Consolidate plants'
    
    return round(score, 1), action, round(yoy_change * 100, 1)


def score_construction_spending(current_value: float, year_ago_value: float) -> Tuple[float, str, float]:
    """
    Score construction spending (highway construction, national proxy).
    Formula: Score = 5.0 + (YoY_change × 15), clamped to 0-10
    """
    if year_ago_value <= 0:
        return 5.0, 'Selective investment', 0.0
    
    yoy_change = (current_value - year_ago_value) / year_ago_value
    raw_score = 5.0 + (yoy_change * 15)
    score = max(0, min(10, raw_score))
    
    if yoy_change > 0.10:
        action = 'All-segment growth'
    elif yoy_change > 0:
        action = 'Selective investment'
    else:
        action = 'Cost focus'
    
    return round(score, 1), action, round(yoy_change * 100, 1)


def score_migration(population_data: Dict[str, Dict]) -> Tuple[float, str, float]:
    """
    Score migration patterns (population-weighted average).
    Formula: Score = 5.0 + (weighted_pct_change × 10), clamped to 0-10
    """
    total_pop = sum(d['population'] for d in population_data.values())
    if total_pop <= 0:
        return 5.0, 'Maintain footprint', 0.0
    
    weighted_change = sum(
        (d['change'] / (d['population'] - d['change'])) * d['population']
        for d in population_data.values()
    ) / total_pop
    
    raw_score = 5.0 + (weighted_change * 10)
    score = max(0, min(10, raw_score))
    
    if weighted_change > 0.01:
        action = 'Geographic expansion'
    elif weighted_change > -0.01:
        action = 'Maintain footprint'
    else:
        action = 'Market consolidation'
    
    return round(score, 1), action, round(weighted_change * 100, 2)


def score_construction_employment(current_total: float, year_ago_total: float) -> Tuple[float, str, float]:
    """
    Score construction employment.
    Formula: Score = 5.0 + (YoY_change × 25), clamped to 0-10
    """
    if year_ago_total <= 0:
        return 5.0, 'Stable operations', 0.0
    
    yoy_change = (current_total - year_ago_total) / year_ago_total
    raw_score = 5.0 + (yoy_change * 25)
    score = max(0, min(10, raw_score))
    
    if score >= 7:
        action = 'Expand workforce'
    elif score >= 4:
        action = 'Stable operations'
    else:
        action = 'Reduce staff'
    
    return round(score, 1), action, round(yoy_change * 100, 1)


def score_input_cost_single(price_history: List[float], baseline: float) -> float:
    """
    Score a single fuel type based on price stability.
    Returns raw score (not clamped).
    """
    if len(price_history) < 2:
        return 5.0
    
    current_price = price_history[-1]
    avg_price = statistics.mean(price_history)
    std_dev = statistics.stdev(price_history) if len(price_history) > 1 else 0
    
    price_ratio = current_price / baseline
    volatility = std_dev / avg_price if avg_price > 0 else 0
    
    stability_factor = 1 / (price_ratio * (1 + volatility))
    return stability_factor * 10


def score_input_cost(fuel_prices: Dict[str, List[float]]) -> Tuple[float, str, Dict]:
    """
    Score input cost stability (combined gasoline + diesel).
    Gasoline weighted 60%, diesel weighted 40%.
    Formula: Score = weighted average of individual fuel scores
    """
    gas_prices = fuel_prices.get('gasoline', [])
    diesel_prices = fuel_prices.get('diesel', [])
    
    # Score each fuel type
    gas_score = score_input_cost_single(gas_prices, BASELINES['gasoline']) if gas_prices else 5.0
    diesel_score = score_input_cost_single(diesel_prices, BASELINES['diesel']) if diesel_prices else 5.0
    
    # Weighted average (gas 60%, diesel 40%)
    gas_weight = INPUT_COST_WEIGHTS['gasoline']
    diesel_weight = INPUT_COST_WEIGHTS['diesel']
    
    combined_score = (gas_score * gas_weight) + (diesel_score * diesel_weight)
    score = max(0, min(10, combined_score))
    
    # Get current prices
    current_gas = gas_prices[-1] if gas_prices else BASELINES['gasoline']
    current_diesel = diesel_prices[-1] if diesel_prices else BASELINES['diesel']
    
    # Determine action
    if score >= 7:
        action = 'Lock contracts'
    elif score >= 4:
        action = 'Hedge 6 months'
    else:
        action = 'Pass-through only'
    
    # Return detailed breakdown
    details = {
        'gasoline': {
            'price': round(current_gas, 2),
            'score': round(min(10, max(0, gas_score)), 1),
            'weight': f"{int(gas_weight * 100)}%"
        },
        'diesel': {
            'price': round(current_diesel, 2),
            'score': round(min(10, max(0, diesel_score)), 1),
            'weight': f"{int(diesel_weight * 100)}%"
        },
        'combined_display': f"Gas ${current_gas:.2f} | Diesel ${current_diesel:.2f}"
    }
    
    return round(score, 1), action, details


def score_input_cost_legacy(price_history: List[float]) -> Tuple[float, str, float]:
    """
    Legacy function for backwards compatibility - diesel only.
    """
    if len(price_history) < 2:
        return 5.5, 'Hedge 6 months', BASELINES['diesel']
    
    baseline_price = BASELINES['diesel']
    current_price = price_history[-1]
    avg_price = statistics.mean(price_history)
    std_dev = statistics.stdev(price_history) if len(price_history) > 1 else 0
    
    price_ratio = current_price / baseline_price
    volatility = std_dev / avg_price if avg_price > 0 else 0
    
    stability_factor = 1 / (price_ratio * (1 + volatility))
    raw_score = stability_factor * 10
    score = max(0, min(10, raw_score))
    
    if score >= 7:
        action = 'Lock contracts'
    elif score >= 4:
        action = 'Hedge 6 months'
    else:
        action = 'Pass-through only'
    
    return round(score, 1), action, round(current_price, 2)


def score_infrastructure_funding() -> Tuple[float, str, float]:
    """
    Score infrastructure funding (IIJA - hardcoded).
    Formula: Score = (funding / $5.5B) × 7.0, clamped to 0-10
    """
    # Determine current fiscal year
    now = datetime.now()
    fy = f"FY{now.year}" if now.month >= 10 else f"FY{now.year}"
    
    funding = IIJA_FUNDING.get(fy, IIJA_FUNDING['FY2025'])
    baseline = BASELINES['infrastructure_funding']
    
    raw_score = (funding / baseline) * 7.0
    score = max(0, min(10, raw_score))
    
    if score >= 7:
        action = 'Major expansion'
    elif score >= 5:
        action = 'Selective growth'
    else:
        action = 'Focus existing assets'
    
    return round(score, 1), action, funding


# =============================================================================
# TREND CALCULATION
# =============================================================================

def calculate_trend(current: float, previous: float, threshold: float = 0.05) -> str:
    """Calculate trend based on percentage change."""
    if previous <= 0:
        return 'stable'
    
    pct_change = (current - previous) / previous
    
    if pct_change > threshold:
        return 'up'
    elif pct_change < -threshold:
        return 'down'
    return 'stable'


# =============================================================================
# CACHE MANAGEMENT
# =============================================================================

def load_cache() -> Dict:
    """Load cached historical data."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {'historical': {}, 'last_values': {}}


def save_cache(cache: Dict):
    """Save cache to file."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


# =============================================================================
# MAIN CALCULATION
# =============================================================================

def calculate_market_health(dot_projects: List[Dict] = None,
                           dot_pipeline_total: float = None, 
                           available_states: int = 4) -> Dict:
    """
    Calculate comprehensive market health scores.
    
    Args:
        dot_projects: List of project dicts from scraper (preferred - enables v2 scoring)
        dot_pipeline_total: Total $ value from DOT scrapers (legacy fallback)
        available_states: Number of states with working scrapers (for legacy extrapolation)
    
    Returns:
        Dict with all market health metrics, scores, trends, and actions
    """
    print("📊 Calculating Market Health Scores (v2.0)...")
    cache = load_cache()
    now = datetime.now()
    
    # Track what data sources succeeded
    data_sources = {}
    
    # -------------------------------------------------------------------------
    # 1. DOT Pipeline (from scraper data) - v2 with time-weighting
    # -------------------------------------------------------------------------
    print("  [1/7] DOT Pipeline...")
    
    dot_details = None  # Will hold v2 detailed breakdown
    
    if dot_projects is not None and len(dot_projects) > 0:
        # V2 SCORING: Use project-level data for time-weighting
        dot_details = score_dot_pipeline_v2(dot_projects, now)
        dot_score = dot_details['score']
        dot_action = dot_details['action']
        dot_pipeline_total = dot_details['extrapolated_weighted']
        
        # Calculate trend from cache
        dot_trend = calculate_trend(dot_pipeline_total, 
                                    cache.get('last_values', {}).get('dot_pipeline', dot_pipeline_total))
        cache.setdefault('last_values', {})['dot_pipeline'] = dot_pipeline_total
        data_sources['dot_pipeline'] = 'scraper_v2'
        
        print(f"    Raw: {dot_details['raw_display']} ({dot_details['coverage']['states_with_data']} states)")
        print(f"    Time-weighted: {dot_details['weighted_display']}")
        print(f"    Extrapolated: {dot_details['extrapolated_display']} ({dot_details['coverage']['market_coverage']} coverage)")
        print(f"    Date coverage: {dot_details['coverage']['date_coverage']}")
        
    elif dot_pipeline_total is not None and dot_pipeline_total > 0:
        # LEGACY: Use simple total (no time-weighting)
        # Apply FHWA-weighted extrapolation instead of naive linear
        states_present = ['MA', 'ME', 'NH', 'CT'][:available_states]  # Assume these states
        captured_ratio = sum(STATE_RATIOS.get(s, 0) for s in states_present)
        if captured_ratio > 0:
            extrapolated = dot_pipeline_total / captured_ratio
            print(f"    Extrapolating via FHWA weights: ${dot_pipeline_total:,.0f} / {captured_ratio:.1%} = ${extrapolated:,.0f}")
            dot_pipeline_total = extrapolated
        
        dot_score, dot_action = score_dot_pipeline(dot_pipeline_total)
        dot_trend = calculate_trend(dot_pipeline_total, 
                                    cache.get('last_values', {}).get('dot_pipeline', dot_pipeline_total))
        cache.setdefault('last_values', {})['dot_pipeline'] = dot_pipeline_total
        data_sources['dot_pipeline'] = 'scraper_legacy'
    else:
        # Use cached or default
        cached_val = cache.get('last_values', {}).get('dot_pipeline', 2_000_000_000)
        dot_score, dot_action = score_dot_pipeline(cached_val)
        dot_trend = 'stable'
        dot_pipeline_total = cached_val
        data_sources['dot_pipeline'] = 'cache'
    
    print(f"    Score: {dot_score}/10 ({dot_trend})")
    
    # -------------------------------------------------------------------------
    # 2. Housing Permits (FRED API)
    # -------------------------------------------------------------------------
    print("  [2/7] Housing Permits...")
    permits_current = 0
    permits_year_ago = 0
    
    for state, series_id in FRED_SERIES['housing_permits'].items():
        data = fetch_fred_series(series_id, limit=24)
        if len(data) >= 13:
            permits_current += data[0]['value']
            permits_year_ago += data[12]['value']
    
    if permits_current > 0:
        permits_score, permits_action, permits_yoy = score_housing_permits(permits_current, permits_year_ago)
        permits_trend = 'up' if permits_yoy > 3 else 'down' if permits_yoy < -3 else 'stable'
        data_sources['housing_permits'] = 'FRED API'
    else:
        permits_score, permits_action, permits_yoy = 6.5, 'Monitor trends', 0.0
        permits_trend = 'stable'
        permits_current = 15000
        data_sources['housing_permits'] = 'fallback'
    
    print(f"    Score: {permits_score}/10 (YoY: {permits_yoy:+.1f}%)")
    
    # -------------------------------------------------------------------------
    # 3. Construction Spending (FRED API)
    # -------------------------------------------------------------------------
    print("  [3/7] Construction Spending...")
    spending_data = fetch_fred_series(FRED_SERIES['construction_spending'], limit=24)
    
    if len(spending_data) >= 13:
        spending_current = spending_data[0]['value']
        spending_year_ago = spending_data[12]['value']
        spending_score, spending_action, spending_yoy = score_construction_spending(spending_current, spending_year_ago)
        spending_trend = 'up' if spending_yoy > 2 else 'down' if spending_yoy < -2 else 'stable'
        data_sources['construction_spending'] = 'FRED API'
    else:
        spending_score, spending_action, spending_yoy = 5.0, 'Selective investment', 0.0
        spending_current = 143000
        spending_trend = 'stable'
        data_sources['construction_spending'] = 'fallback'
    
    print(f"    Score: {spending_score}/10 (YoY: {spending_yoy:+.1f}%)")
    
    # -------------------------------------------------------------------------
    # 4. Migration Patterns (Census API)
    # -------------------------------------------------------------------------
    print("  [4/7] Migration Patterns...")
    pop_data = fetch_census_population()
    
    if pop_data:
        migration_score, migration_action, migration_pct = score_migration(pop_data)
        migration_trend = 'up' if migration_pct > 0.3 else 'down' if migration_pct < -0.3 else 'stable'
        total_pop = sum(d['population'] for d in pop_data.values())
        data_sources['migration'] = 'Census API'
    else:
        migration_score, migration_action, migration_pct = 5.0, 'Maintain footprint', 0.0
        migration_trend = 'stable'
        total_pop = 47_710_000
        data_sources['migration'] = 'fallback'
    
    print(f"    Score: {migration_score}/10 (Change: {migration_pct:+.2f}%)")
    
    # -------------------------------------------------------------------------
    # 5. Construction Employment (FRED API)
    # -------------------------------------------------------------------------
    print("  [5/7] Construction Employment...")
    employment_current = 0
    employment_year_ago = 0
    
    for state, series_id in FRED_SERIES['construction_employment'].items():
        data = fetch_fred_series(series_id, limit=24)
        if len(data) >= 13:
            employment_current += data[0]['value']
            employment_year_ago += data[12]['value']
    
    if employment_current > 0:
        employment_score, employment_action, employment_yoy = score_construction_employment(employment_current, employment_year_ago)
        employment_trend = 'up' if employment_yoy > 2 else 'down' if employment_yoy < -2 else 'stable'
        data_sources['construction_employment'] = 'FRED API'
    else:
        employment_score, employment_action, employment_yoy = 5.0, 'Stable operations', 0.0
        employment_current = 875
        employment_trend = 'stable'
        data_sources['construction_employment'] = 'fallback'
    
    print(f"    Score: {employment_score}/10 (YoY: {employment_yoy:+.1f}%)")
    
    # -------------------------------------------------------------------------
    # 6. Input Cost Stability (EIA API - Gas + Diesel)
    # -------------------------------------------------------------------------
    print("  [6/7] Input Cost Stability...")
    fuel_prices = fetch_eia_fuel_prices(12)
    
    if fuel_prices.get('gasoline') or fuel_prices.get('diesel'):
        input_score, input_action, input_details = score_input_cost(fuel_prices)
        
        # Calculate trend based on weighted price change
        gas_prices = fuel_prices.get('gasoline', [])
        diesel_prices = fuel_prices.get('diesel', [])
        
        if len(gas_prices) >= 5 and len(diesel_prices) >= 5:
            # Weighted current vs 4 weeks ago
            current_weighted = (gas_prices[-1] * 0.60) + (diesel_prices[-1] * 0.40)
            past_weighted = (gas_prices[-5] * 0.60) + (diesel_prices[-5] * 0.40)
            # For input cost, lower is better, so flip the trend logic
            input_trend = 'up' if current_weighted < past_weighted - 0.08 else 'down' if current_weighted > past_weighted + 0.08 else 'stable'
        else:
            input_trend = 'stable'
        
        data_sources['input_cost'] = 'EIA API (Gas + Diesel)' if EIA_API_KEY else 'historical data'
        current_gas = input_details['gasoline']['price']
        current_diesel = input_details['diesel']['price']
    else:
        input_score, input_action = 5.5, 'Hedge 6 months'
        input_details = {
            'gasoline': {'price': 3.20, 'score': 5.5, 'weight': '60%'},
            'diesel': {'price': 4.00, 'score': 5.5, 'weight': '40%'},
            'combined_display': 'Gas $3.20 | Diesel $4.00'
        }
        input_trend = 'stable'
        current_gas = 3.20
        current_diesel = 4.00
        data_sources['input_cost'] = 'fallback'
    
    print(f"    Score: {input_score}/10 (Gas: ${current_gas:.2f}, Diesel: ${current_diesel:.2f})")
    
    # -------------------------------------------------------------------------
    # 7. Infrastructure Funding (Hardcoded IIJA)
    # -------------------------------------------------------------------------
    print("  [7/7] Infrastructure Funding...")
    funding_score, funding_action, funding_amount = score_infrastructure_funding()
    funding_trend = 'up'  # IIJA is increasing through FY2026
    data_sources['infrastructure_funding'] = 'IIJA hardcoded'
    
    print(f"    Score: {funding_score}/10 (${funding_amount/1e9:.1f}B)")
    
    # -------------------------------------------------------------------------
    # OVERALL SCORE
    # -------------------------------------------------------------------------
    scores = {
        'dot_pipeline': dot_score,
        'housing_permits': permits_score,
        'construction_spending': spending_score,
        'migration': migration_score,
        'construction_employment': employment_score,
        'input_cost': input_score,
        'infrastructure_funding': funding_score,
    }
    
    weighted_sum = sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)
    total_weight = sum(WEIGHTS.values())
    overall_score = round(weighted_sum / total_weight, 1)
    
    if overall_score >= 7.6:
        overall_status = 'growth'
    elif overall_score >= 6.1:
        overall_status = 'stable'
    elif overall_score >= 5.0:
        overall_status = 'watchlist'
    else:
        overall_status = 'defensive'
    
    print(f"\n  📈 Overall Score: {overall_score}/10 ({overall_status.upper()})")
    
    # -------------------------------------------------------------------------
    # BUILD RESULT
    # -------------------------------------------------------------------------
    
    # DOT pipeline result - include v2 details if available
    dot_result = {
        'score': dot_score,
        'trend': dot_trend,
        'action': dot_action,
        'raw': dot_pipeline_total,
        'raw_display': f"${dot_pipeline_total/1e9:.2f}B" if dot_pipeline_total >= 1e9 else f"${dot_pipeline_total/1e6:.1f}M",
        'source': data_sources['dot_pipeline'],
        'updated': now.isoformat()
    }
    
    # Add v2 detailed breakdown if available
    if dot_details:
        dot_result['v2_details'] = {
            'raw_total': dot_details['raw_display'],
            'weighted_total': dot_details['weighted_display'],
            'extrapolated_total': dot_details['extrapolated_display'],
            'by_horizon': dot_details['by_horizon'],
            'by_state': dot_details['by_state'],
            'coverage': dot_details['coverage'],
            'scoring_params': dot_details['scoring_params']
        }
    
    result = {
        'dot_pipeline': dot_result,
        'housing_permits': {
            'score': permits_score,
            'trend': permits_trend,
            'action': permits_action,
            'raw': permits_current,
            'raw_display': f"{permits_current:,.0f} units/mo",
            'yoy_change': permits_yoy,
            'source': data_sources['housing_permits'],
            'updated': now.isoformat()
        },
        'construction_spending': {
            'score': spending_score,
            'trend': spending_trend,
            'action': spending_action,
            'raw': spending_current if 'spending_current' in dir() else 143000,
            'raw_display': f"${spending_current/1000:.1f}B SAAR" if 'spending_current' in dir() else "$143B SAAR",
            'yoy_change': spending_yoy,
            'source': data_sources['construction_spending'],
            'updated': now.isoformat()
        },
        'migration': {
            'score': migration_score,
            'trend': migration_trend,
            'action': migration_action,
            'raw': total_pop if 'total_pop' in dir() else 47710000,
            'raw_display': f"{total_pop/1e6:.1f}M people" if 'total_pop' in dir() else "47.7M people",
            'pct_change': migration_pct,
            'source': data_sources['migration'],
            'updated': now.isoformat()
        },
        'construction_employment': {
            'score': employment_score,
            'trend': employment_trend,
            'action': employment_action,
            'raw': employment_current * 1000,  # FRED is in thousands
            'raw_display': f"{employment_current:.0f}K workers",
            'yoy_change': employment_yoy,
            'source': data_sources['construction_employment'],
            'updated': now.isoformat()
        },
        'input_cost': {
            'score': input_score,
            'trend': input_trend,
            'action': input_action,
            'raw_display': input_details.get('combined_display', f"Gas ${current_gas:.2f} | Diesel ${current_diesel:.2f}"),
            'gasoline': input_details.get('gasoline', {'price': current_gas, 'score': 5.0, 'weight': '60%'}),
            'diesel': input_details.get('diesel', {'price': current_diesel, 'score': 5.0, 'weight': '40%'}),
            'source': data_sources['input_cost'],
            'updated': now.isoformat()
        },
        'infrastructure_funding': {
            'score': funding_score,
            'trend': funding_trend,
            'action': funding_action,
            'raw': funding_amount,
            'raw_display': f"${funding_amount/1e9:.1f}B",
            'source': data_sources['infrastructure_funding'],
            'updated': now.isoformat()
        },
        'overall_score': overall_score,
        'overall_status': overall_status,
        'data_sources': data_sources,
        'calculated_at': now.isoformat()
    }
    
    # Save cache
    save_cache(cache)
    
    return result


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("NECMIS Market Health Engine v1.0 - Test Run")
    print("=" * 60)
    print()
    
    # Check for API keys
    if not FRED_API_KEY:
        print("⚠️  FRED_API_KEY not set - will use fallback values")
        print("   Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
    if not EIA_API_KEY:
        print("⚠️  EIA_API_KEY not set - will use historical diesel data")
        print("   Get a free key at: https://www.eia.gov/opendata/register.php")
    print()
    
    # Test with sample DOT pipeline value
    mh = calculate_market_health(dot_pipeline_total=150_000_000, available_states=4)
    
    print()
    print("=" * 60)
    print("FULL RESULT:")
    print("=" * 60)
    print(json.dumps(mh, indent=2, default=str))
