# NECMIS - Northeast Construction Market Intelligence System

Automated daily intelligence for highway construction, aggregates, HMA, ready-mix concrete, and trucking markets across VT, NH, ME, NY, PA, MA, RI, CT.

## Quick Start (~15 minutes)

### Step 1: Create GitHub Account (if needed)
Go to [github.com](https://github.com) and sign up

### Step 2: Create Your Repository
1. Click the green **"Use this template"** button (top right) OR:
   - Go to github.com/new
   - Name it: `necmis` (or whatever you want)
   - Make it **Private** (recommended for team use)
   - Click **Create repository**

2. Upload these files to your repo:
   - `scraper.py`
   - `index.html`
   - `.github/workflows/scrape.yml`
   - `data/necmis_data.json` (sample data)

### Step 3: Enable GitHub Actions
1. Go to your repo → **Settings** → **Actions** → **General**
2. Under "Actions permissions", select **"Allow all actions"**
3. Under "Workflow permissions", select **"Read and write permissions"**
4. Click **Save**

### Step 4: Enable GitHub Pages
1. Go to your repo → **Settings** → **Pages**
2. Under "Source", select **"Deploy from a branch"**
3. Select branch: **main**, folder: **/ (root)**
4. Click **Save**
5. Wait 2-3 minutes, then your dashboard will be at:
   `https://YOUR-USERNAME.github.io/necmis/`

### Step 5: Run First Scrape
1. Go to your repo → **Actions** tab
2. Click **"NECMIS Daily Scrape"** on the left
3. Click **"Run workflow"** button (right side)
4. Wait ~2 minutes for it to complete
5. Check `data/necmis_data.json` - it should have real data

### Step 6: Share With Your Team
1. Open `index.html` and change the password on line ~320:
   ```javascript
   const TEAM_PASSWORD = 'your-secure-password';
   ```
2. Commit the change
3. Share the URL with your team: `https://YOUR-USERNAME.github.io/necmis/`

## How It Works

```
6 AM EST Daily
     │
     ▼
┌─────────────────────┐
│  GitHub Actions     │
│  runs scraper.py    │
│                     │
│  • 14 RSS feeds     │
│  • 8 DOT pages      │
│  • Filters/scores   │
└─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  data/necmis_data   │
│  .json updated      │
└─────────────────────┘
     │
     ▼
┌─────────────────────┐
│  Team opens         │
│  dashboard URL      │
│  (index.html)       │
│                     │
│  • Password gate    │
│  • Filter by state  │
│  • Filter by biz    │
│  • Click to source  │
└─────────────────────┘
```

## Data Sources

### Regional News (RSS)
| Source | State | Feed |
|--------|-------|------|
| VTDigger | VT | vtdigger.org/feed |
| Vermont Biz | VT | vermontbiz.com/feed |
| Union Leader | NH | unionleader.com RSS |
| InDepthNH | NH | indepthnh.org/feed |
| Press Herald | ME | pressherald.com/feed |
| Bangor Daily | ME | bangordailynews.com/feed |
| Times Union | NY | timesunion.com RSS |
| Syracuse.com | NY | syracuse.com RSS |
| PennLive | PA | pennlive.com RSS |
| MassLive | MA | masslive.com RSS |
| Providence Journal | RI | providencejournal.com RSS |
| CT Mirror | CT | ctmirror.org/feed |

### Industry Publications (RSS)
| Source | Coverage | Feed |
|--------|----------|------|
| Pit & Quarry | Aggregates | pitandquarry.com/feed |
| ForConstructionPros | Concrete/General | forconstructionpros.com/rss |

### DOT Pages (HTML scraping)
| Agency | URL |
|--------|-----|
| VTrans | vtrans.vermont.gov/contract-admin/bids-requests |
| NHDOT | dot.nh.gov/doing-business-nhdot/contractors/invitation-bid |
| MaineDOT | maine.gov/dot/projects |
| NYSDOT | dot.ny.gov/doing-business/opportunities/const-highway |
| MassDOT | mass.gov/info-details/advertised-projects-bid-opening-schedule |
| RIDOT | dot.ri.gov/projects |
| CTDOT | portal.ct.gov/dot/projects |

## Business Lines Tracked

- **Highway** - road construction, paving, DOT projects
- **HMA** - hot mix asphalt, bituminous, overlay
- **Aggregates** - quarry, gravel, sand, stone, crusher
- **Concrete** - ready-mix, cement, batch plant
- **Liquid Asphalt** - bitumen, emulsion, binder
- **Trucking** - hauling, dump truck, fleet

## Configuration

### Change Scrape Schedule
Edit `.github/workflows/scrape.yml`:
```yaml
schedule:
  - cron: '0 11 * * *'  # 11:00 UTC = 6 AM EST
```

### Add Keywords
Edit `scraper.py`, add to keyword lists:
```python
KEYWORDS_AGGREGATES = [
    'aggregate', 'quarry', ...
    'your-new-keyword',  # add here
]
```

### Add RSS Feeds
Edit `scraper.py`:
```python
RSS_FEEDS = {
    ...
    'New Source': {'url': 'https://example.com/feed/', 'state': 'VT'},
}
```

## Limitations (Being Honest)

- **~70-80% capture rate** - RSS feeds don't publish everything
- **Daily updates only** - not real-time
- **Some DOT sites may block** - handled gracefully, will show as failed
- **News focus** - won't capture every bid (DOT sites best for that)
- **PennDOT ECMS** - requires login, limited public data available

## Troubleshooting

### Scraper fails with 403 errors
Some sites block automated requests. The scraper handles this gracefully and continues with other sources.

### No new data
Check Actions tab for workflow run logs. Most common issues:
- Rate limiting (wait and retry)
- Site structure changed (may need scraper update)

### Dashboard shows "Failed to load"
- Check that `data/necmis_data.json` exists
- Ensure GitHub Pages is enabled
- Clear browser cache

## Cost

**$0/month** - Uses only:
- GitHub Actions (free tier: 2000 minutes/month)
- GitHub Pages (free for public/private repos)
- Public RSS feeds (no API keys needed)

## Files

```
necmis/
├── .github/
│   └── workflows/
│       └── scrape.yml      # Automated daily scrape
├── data/
│   └── necmis_data.json    # Output data (auto-updated)
├── index.html              # Dashboard (password protected)
├── scraper.py              # Python scraper
└── README.md               # This file
```

## Support

For issues:
1. Check Actions logs for error messages
2. Verify source URLs still work
3. Open an issue in this repo

---

Built for construction market intelligence in the Northeast.
