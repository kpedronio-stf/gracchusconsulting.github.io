AUTHOR = 'Michael Frank'
SITENAME = 'Gracchus Consulting'
SITEURL = 'gracchusconsulting.com'

PATH = 'content'
STATIC_PATHS = ['images']

TIMEZONE = 'America/Los_Angeles'

DEFAULT_LANG = 'en'

THEME = 'gracchus'

# Disable all feed generation
FEED_ALL_ATOM = None
CATEGORY_FEED_ATOM = None
TRANSLATION_FEED_ATOM = None
AUTHOR_FEED_ATOM = None
AUTHOR_FEED_RSS = None

# Blogroll
LINKS = ()

# Social widget
SOCIAL = ()

DEFAULT_PAGINATION = 10

ARTICLE_SAVE_AS = 'content/{slug}.html'
ARTICLE_URL = 'content/{slug}.html'
PAGE_SAVE_AS = '{slug}.html'
PAGE_URL = '{slug}.html'

# true if you want document-relative URLs when developing
RELATIVE_URLS = False
