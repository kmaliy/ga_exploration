# API profiling report

## Field profile
Profiled 600 records across ['20160801', '20161123', '20161128', '20161225', '20170315', '20170716'].

| field | present % | null % (when present) | types | range / sample values |
|---|---|---|---|---|
| `channelGrouping` | 100.0 | 0.0 | str | Affiliates, Direct, Display, Organic Search, Paid Search, Referral |
| `customDimensions` | 100.0 | 0.0 | list | <list len=0>, <list len=1> |
| `date` | 100.0 | 0.0 | str | 20160801, 20161123, 20161128, 20161225, 20170315, 20170716 |
| `device` | 100.0 | 0.0 | dict |  |
| `device.browser` | 100.0 | 0.0 | str | Amazon Silk, Android Webview, Chrome, Edge, Firefox, Internet Explorer |
| `device.isMobile` | 100.0 | 0.0 | bool | False, True |
| `device.operatingSystem` | 100.0 | 0.0 | str | (not set), Android, Chrome OS, Linux, Macintosh, Windows |
| `fullVisitorId` | 100.0 | 0.0 | str | 0403454456389348844, 1644562297167206343, 2090456244473009269, 3065316533128123013, 3616491314110893756, 3634155159894530540 |
| `geoNetwork` | 100.0 | 0.0 | dict |  |
| `geoNetwork.city` | 100.0 | 0.0 | str | (not set), Bengaluru, Cebu City, Dubai, Hanoi, Hyderabad |
| `geoNetwork.cityId` | 100.0 | 0.0 | str | not available in demo dataset |
| `geoNetwork.continent` | 100.0 | 0.0 | str | Africa, Americas, Asia, Europe, Oceania |
| `geoNetwork.country` | 100.0 | 0.0 | str | Albania, Australia, Bulgaria, Canada, Cyprus, Hong Kong |
| `geoNetwork.latitude` | 100.0 | 0.0 | str | not available in demo dataset |
| `geoNetwork.longitude` | 100.0 | 0.0 | str | not available in demo dataset |
| `geoNetwork.metro` | 100.0 | 0.0 | str | (not set), Chicago IL, Dallas-Ft. Worth TX, Denver CO, JP_KANTO, Los Angeles CA |
| `geoNetwork.networkDomain` | 100.0 | 0.0 | str | (not set), 111-tataidc.co.in, airtelbroadband.in, bigpond.net.au, com, cox.net |
| `geoNetwork.networkLocation` | 100.0 | 0.0 | str | not available in demo dataset |
| `geoNetwork.region` | 100.0 | 0.0 | str | (not set), Bangkok, California, Central Visayas, Delhi, Dubai |
| `geoNetwork.subContinent` | 100.0 | 0.0 | str | Australasia, Caribbean, Central America, Eastern Asia, Eastern Europe, Northern Africa |
| `hits_sample` | 100.0 | 0.0 | list | <list len=1>, <list len=2>, <list len=3> |
| `totals` | 100.0 | 0.0 | dict |  |
| `totals.bounces` | 100.0 | 51.2 | int | [1, 1] |
| `totals.hits` | 100.0 | 0.0 | int | [1, 67] |
| `totals.newVisits` | 100.0 | 18.2 | int | [1, 1] |
| `totals.pageviews` | 100.0 | 0.0 | int | [1, 46] |
| `totals.visits` | 100.0 | 0.0 | int | [1, 1] |
| `trafficSource` | 100.0 | 0.0 | dict |  |
| `trafficSource.adContent` | 100.0 | 99.3 | str | 20% discount, Display Ad created 3/11/14, Display Ad created 3/11/15 |
| `trafficSource.keyword` | 100.0 | 71.8 | str | (Remarketing/Content targeting), (not provided), 1X4Me6ZKNV0zg-jV, 6qEhsCssdK0z36ri, category_l1==*, google merchandise store |
| `trafficSource.medium` | 100.0 | 0.0 | str | (none), affiliate, cpc, cpm, organic, referral |
| `trafficSource.referralPath` | 100.0 | 53.3 | str | /, /2015/03/11/google-merch-store-new-url/, /YKEI_mrn/items/c10b14f9a69ff71b1b7a, /analytics/web/, /permissions/using-the-logo.html, /yt/about/ |
| `trafficSource.source` | 100.0 | 0.0 | str | (direct), Partners, analytics.google.com, baidu, bing, dfa |
| `visitId` | 100.0 | 0.0 | int | [1470114566, 1500274756] |
| `visitNumber` | 100.0 | 0.0 | int | [1, 52] |
| `visitStartTime` | 100.0 | 0.0 | int | [1470114566, 1500274756] |

Transaction examples found: 0 (revenue fields never observed)

## Limit ceiling
```json
{
  "limit=5000": {
    "status": 200,
    "seconds": 3.27,
    "records_returned": 500,
    "pagination": {
      "has_next": true,
      "has_previous": false,
      "limit": 500,
      "page": 1,
      "total_pages": 6,
      "total_records": 2556
    }
  },
  "limit=0": {
    "status": 400,
    "seconds": 0.15,
    "records_returned": null,
    "pagination": null
  }
}
```

## Ordering stability
```json
{
  "same_query_identical_order": true,
  "page1_page2_overlap": [],
  "page1_ids": [
    1501657193,
    1501657190,
    1501657186,
    1501657166,
    1501657161
  ]
}
```

## Rate-limit burst
```json
{
  "burst_size": 20,
  "status_counts": {
    "200": 20
  },
  "saw_429": false,
  "retry_after_header_seen": false,
  "latency_seconds": {
    "min": 2.28,
    "median": 2.87,
    "max": 3.82
  }
}
```
