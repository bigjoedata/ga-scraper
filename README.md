# ga-scraper

This is a quick & dirty way to pull GA data & store on your data lake.

Experimentation, trial & error led me to boil down to boil GA down to two distinct groups of reports: Events related & non events related.  There's also a max 10 dimensions & 20 metrics in any given report. These scripts are my best stab at getting the most of each. No PII is transferred (Not that GA even has any).

Make sure to create the "_Events" & "_Non_Events" folders or change path in the pandas to_csv function

Make sure you have the dependencies listed & configure the config.py with creds

It will work with Python 3.7x & might even work with lower versions. Try & find out.

Sample command line call: python ga_dl-NON_EVENTS.py Report_Name VIEWID YYYY-MM-DD

Yes, this is a single day's analytics, so you will need to call for each day, each suite, etc. Paging isn't implemented. So, if any of your calls have >100K lines, you will need to implement paging. Easy to add, I just didn't due to lack of necessity.

This is one of the most straightforward guides I’ve seen for getting google authentication
https://community.alteryx.com/t5/Alteryx-Knowledge-Base/The-How-to-Guide-to-Google-Analytics/ta-p/15137
