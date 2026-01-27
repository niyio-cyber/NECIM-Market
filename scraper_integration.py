#!/usr/bin/env python3
"""
NECMIS Scraper Integration - How to Add Market Health Engine
=============================================================

This file shows the changes needed to integrate market_health_engine.py
into your existing scraper.py

OPTION 1: Minimal Change (Recommended)
--------------------------------------
Add these lines to the end of your scraper.py run_scraper() function,
replacing the existing calculate_market_health() call.

OPTION 2: Full Replacement
--------------------------
Replace your entire calculate_market_health() function with an import.
"""

# =============================================================================
# OPTION 1: Minimal Integration (Add to scraper.py)
# =============================================================================

# Step 1: Add import at top of scraper.py
"""
# Add this import near the top of scraper.py
try:
    from market_health_engine import calculate_market_health as calculate_real_market_health
    USE_REAL_MARKET_HEALTH = True
except ImportError:
    USE_REAL_MARKET_HEALTH = False
    print("⚠️  market_health_engine.py not found, using basic scoring")
"""

# Step 2: Replace the market health section in run_scraper() 
"""
# In run_scraper(), replace this:
    print("[3/3] Market Health...")
    mh = calculate_market_health(dot_lettings, news)
    
# With this:
    print("[3/3] Market Health...")
    if USE_REAL_MARKET_HEALTH:
        # Calculate total pipeline value from DOT lettings
        total_pipeline = sum(d.get('cost_low') or 0 for d in dot_lettings)
        
        # Count working states (those with actual data)
        active_states = len(set(d['state'] for d in dot_lettings if d.get('cost_low')))
        
        # Use real market health engine
        mh = calculate_real_market_health(
            dot_pipeline_total=total_pipeline,
            available_states=max(1, active_states)
        )
    else:
        # Fallback to basic scoring
        mh = calculate_market_health(dot_lettings, news)
"""


# =============================================================================
# OPTION 2: Complete scraper.py replacement section
# =============================================================================

# Here's the full updated run_scraper() function that uses the market health engine:

def run_scraper_with_market_health() -> dict:
    """
    Updated run_scraper() that integrates the real market health engine.
    Copy this function into your scraper.py and rename it to run_scraper().
    """
    from datetime import datetime, timezone
    import json
    import os
    
    # Try to import market health engine
    try:
        from market_health_engine import calculate_market_health as calc_real_mh
        use_real_mh = True
    except ImportError:
        use_real_mh = False
    
    print("=" * 60)
    print("NECMIS SCRAPER - PHASE 6.0 (Real Market Health)")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # These functions come from your existing scraper.py
    # (assuming they're defined elsewhere in the file)
    from scraper import fetch_dot_lettings, fetch_rss_feeds, build_summary, format_currency
    
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
    if use_real_mh:
        # Count active states
        active_states = len(set(d['state'] for d in dot_lettings if d.get('cost_low')))
        active_states = max(1, active_states)  # At least 1
        
        # Call real market health engine
        mh = calc_real_mh(
            dot_pipeline_total=total_val,
            available_states=active_states
        )
        print(f"  ✅ Using REAL market health data")
    else:
        # Fallback to basic (your existing function)
        from scraper import calculate_market_health as calc_basic_mh
        mh = calc_basic_mh(dot_lettings, news)
        print(f"  ⚠️  Using BASIC market health (hardcoded)")
    
    print(f"  Score: {mh.get('overall_score', '--')}/10 ({mh.get('overall_status', '--').upper()})")
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
    
    # Show market health breakdown
    print("\nMarket Health Breakdown:")
    for metric in ['dot_pipeline', 'housing_permits', 'construction_spending', 
                   'migration', 'construction_employment', 'input_cost', 'infrastructure_funding']:
        m = mh.get(metric, {})
        score = m.get('score', '--')
        source = m.get('source', 'unknown')
        print(f"  {metric}: {score}/10 ({source})")
    
    print("=" * 60)
    
    return data


# =============================================================================
# ENVIRONMENT SETUP (Required for APIs)
# =============================================================================

"""
To enable real API data, set these environment variables:

1. FRED API (free): https://fred.stlouisfed.org/docs/api/api_key.html
   export FRED_API_KEY="your_key_here"

2. EIA API (free): https://www.eia.gov/opendata/register.php
   export EIA_API_KEY="your_key_here"

3. Census API: No key required for low volume

For GitHub Actions, add these as repository secrets:
- FRED_API_KEY
- EIA_API_KEY

Then update your scrape.yml:

    - name: Run scraper
      env:
        FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
        EIA_API_KEY: ${{ secrets.EIA_API_KEY }}
      run: python scraper.py
"""


# =============================================================================
# QUICK TEST
# =============================================================================

if __name__ == '__main__':
    print("This file shows how to integrate market_health_engine.py")
    print("See the comments above for integration instructions.")
    print()
    print("To test the market health engine directly, run:")
    print("  python market_health_engine.py")
