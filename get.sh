curl -XGET 'http://localhost:9200/store/_search?pretty=true' -d '
{
  "query": {
    "filtered" : {
        "query" : {
            "match_all" : {}
        },
        "filter" : {
            "geo_distance" : {
                "distance" : "20km",
                "location" : {
                    "lat" : 37.9174,
                    "lon" : -122.3050
                }
            }
        }
    }
  }
}'
