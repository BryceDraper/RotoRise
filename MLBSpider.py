import scrapy
from datetime import datetime
import datetime
from scrapy_splash import SplashRequest
from dateutil import tz
import csv
from scrapy.crawler import CrawlerProcess


class MLBSpider(scrapy.Spider):
    name = 'MLBSpider'
    SPLASH_ARGS = {
        'wait': 20.0,
        'images': 0,
        'render_all': 1,
        'timeout': 40.0,
        'resource_timeout': 40.0}

    def start_requests(self):
        url = 'http://www.espn.com/mlb/schedule'
        yield SplashRequest(url, callback=self.parse, args=dict(self.SPLASH_ARGS))

    def parse(self, response):

        #Scrapes Home Team Name and appends to team list
        teams = []
        for team in response.xpath('//div[@class="responsive-table-wrap"][2]/table/tbody/tr/td[@class="home"]/div/a/abbr'):
            team = team.xpath('@title').extract_first()
            teams.append(team)

        # Scrapes game start time and converts from UTC to MST, then appends to times list
        times = []
        for starttime in response.xpath('//div[@class="responsive-table-wrap"][2]/table/tbody/tr/td[@data-behavior="date_time"]'):
            st_time = starttime.xpath('@data-date').extract_first()
            st_time = st_time[st_time.find('T')+1:-1]
            utc = datetime.datetime.strptime(datetime.date.today().strftime('%Y-%m-%d') + ' ' + st_time, '%Y-%m-%d %H:%M')
            utc = utc.replace(tzinfo=tz.gettz('UTC'))
            new_time = utc.astimezone(tz.gettz('America/Denver'))
            new_time = new_time.strftime('%H:%M')
            times.append(new_time)

        # Writes home team and start time to csv as "Team","Time"
        with open('/home/ec2-user/schedule.csv', 'w') as output:
            writer = csv.writer(output, lineterminator='\n')
            for x, y in zip(teams, times):
                writer.writerow([x] + [y])

    pass

# Allows spider to be run by calling file instead of using scrapy commands
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': "@RotoRise Podcast's Friendly Neighborhood Spider",
        'HTTPCACHE_ENABLED': False
    })

    process.crawl(MLBSpider)
    process.start()