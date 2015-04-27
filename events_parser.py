import feedparser


feed_url="https://ca.evanced.info/mountainview/lib/eventsxml.asp?ag=&et=&lib=0&nd=30&feedtitle=Mountain+View+Public+Library%3CBR%3ECalendar+of+Events%2C+Programs+and+Storytimes&dm=rss2&LangType=0"

feed_url="https://library.sjsu.edu/events/events-feed"

d = feedparser.parse(feed_url)
print d['feed']['title']
print d['feed']['link']
print d.feed.subtitle

for entry in d['entries']:
    print entry['title']
    print entry['summary']


