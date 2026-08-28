#!/usr/bin/env python3
"""Assessment entry point (Step 2 deliverable).

The implementation lives in the ``ga_pipeline`` package; this module is the
stable, documented entry point:

    python data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07
    python data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run
    python data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-31

Configuration is environment-only (see .env.example). Never pass secrets as
CLI arguments — they leak into shell history and process listings.
"""

import sys

from ga_pipeline.cli import main

if __name__ == "__main__":
    sys.exit(main())
