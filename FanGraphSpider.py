import scrapy
import tweepy
import datetime
from scrapy_splash import SplashRequest
import csv
from scrapy.crawler import CrawlerProcess


tkey = 'Redacted'
tsecret = 'Redacted'
atoke = 'Redacted'
asecret = 'Redacted'

teamdict = {
    'Atlanta Braves': 'Braves',
    'Miami Marlins': 'Marlins',
    'New York Mets': 'Mets',
    'Philadelphia Phillies': 'Phillies',
    'Washington Nationals': 'Nationals',
    'Chicago Cubs': 'Cubs',
    'Cincinnati Reds': 'Reds',
    'Milwaukee Brewers': 'Brewers',
    'Pittsburgh Pirates': 'Pirates',
    'St. Louis Cardinals': 'Cardinals',
    'Arizona Diamondbacks': 'Diamondbacks',
    'Colorado Rockies': 'Rockies',
    'Los Angeles Dodgers': 'Dodgers',
    'San Diego Padres': 'Padres',
    'San Francisco Giants': 'Giants',
    'Baltimore Orioles': 'Orioles',
    'Boston Red Sox': 'Red%20Sox',
    'New York Yankees': 'Yankees',
    'Tampa Bay Rays': 'Rays',
    'Toronto Blue Jays': 'Blue%20Jays',
    'Chicago White Sox': 'White%20Sox',
    'Cleveland Indians': 'Indians',
    'Detroit Tigers': 'Tigers',
    'Kansas City Royals': 'Royals',
    'Minnesota Twins': 'Twins',
    'Houston Astros': 'Astros',
    'Los Angeles Angels': 'Angels',
    'Oakland Athletics': 'Athletics',
    'Seattle Mariners': 'Mariners',
    'Texas Rangers': 'Rangers',
}

#Builds URLs to be scraped from team name and date. Team Name Gathered from MLB SPider.
starturls = []
dt = datetime.date.today().strftime('%Y-%m-%d')
with open('schedule.csv', 'r') as input:
    reader = csv.reader(input)
    for item in reader:
        # Only adds item to url if time is later than scheduled game time
        if item[0] in teamdict.keys() and item[1] < datetime.datetime.now().strftime('%H:%M'):
            starturls.append('https://www.fangraphs.com/livewins.aspx?date=%s&team=%s&dh=0&season=%s' % (dt, teamdict[item[0]], dt[:4]))


class FanGraphSpider(scrapy.Spider):
    name = 'FanGraphSpider'
    start_urls = starturls
    SPLASH_ARGS = {
        'wait': 20.0,
        'images': 0,
        'render_all': 1,
        'timeout': 40.0,
        'resource_timeout': 40.0}

    def parse(self, response):
        # Checks for Home Runs and stolen bases in play log and, if found, sends player stat page to parser.
        tweeted = False
        for item in response.xpath('//div[@id="LiveGame1_dgPlay"]/table/tbody/tr'):
            if 'homered' in item.xpath('td[6]/text()').extract_first():
                # Checks to see if play has already been sent to parser. If not, adds it to list to avoid duplicates.
                plname = item.xpath('td[2]/text()').extract_first()
                with open('dupcatcher.csv', 'r') as input:
                    reader = csv.reader(input)
                    for line in reader:
                        if [item.xpath('td[6]/text()').extract_first()] == line:
                            tweeted = True
                if not tweeted:
                    with open('dupcatcher.csv', 'a') as output:
                        writer = csv.writer(output)
                        line = item.xpath('td[6]/text()').extract_first()
                        writer.writerow([line])
                    for namespot in response.xpath('//div[contains(@id, "dg3")]/table/tbody/tr/td[1] | //div[contains(@id, "dg4")]/table/tbody/tr/td[1]'):
                        name = namespot.xpath('a/text()').extract_first()
                        if plname == name:
                            url = namespot.xpath('a/@href').extract_first()
                            break
                    url = response.urljoin(url)
                    yield SplashRequest(url, callback=self.home_parse, args=dict(self.SPLASH_ARGS))
            if 'stolen' in item.xpath('td[6]/text()').extract_first():
                plname = item.xpath('td[2]/text()').extract_first()
                # Checks to see if play has already been sent to parser. If not, adds it to list to avoid duplicates.
                with open('dupcatcher.csv', 'r') as input:
                    reader = csv.reader(input)
                    for line in reader:
                        if [item.xpath('td[6]/text()').extract_first()] == line:
                            tweeted = True
                if not tweeted:
                    with open('dupcatcher.csv', 'a') as output:
                        writer = csv.writer(output, lineterminator='\n')
                        line = item.xpath('td[6]/text()').extract_first()
                        writer.writerow([line])
                    for namespot in response.xpath('//div[contains(@id, "dg3")]/table/tbody/tr/td[1] | //div[contains(@id, "dg4")]/table/tbody/tr/td[1]'):
                        name = namespot.xpath('a/text()').extract_first()
                        if plname == name:
                            url = namespot.xpath('a/@href').extract_first()
                            break
                    url = response.urljoin(url)
                    yield SplashRequest(url, callback=self.steal_parse, args=dict(self.SPLASH_ARGS))

    # Gathers player stats and constructs tweets for homeruns
    def home_parse(self, response):
        p_name = response.xpath('//div[@class="player-info-box-name"]/h1/text()').extract_first()
        team = response.xpath('//div[@class="player-info-box-name-team"]/a/text()').extract_first()
        av = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[17]/text()').extract_first()
        rstat = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[7]/text()').extract_first()
        hr = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[6]/text()').extract_first()
        rbi = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[8]/text()').extract_first()
        sb = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[9]/text()').extract_first()
        obp = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[18]/text()').extract_first()
        slg = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[19]/text()').extract_first()
        OPS = str(float(obp)+float(slg))

        tweet = u'\U0001F929' + ' Home Run! ' + u'\U0001F929' + '- %s - %s \n Season Totals \n %s AVG, %s R, %s HR, %s RBI, %s SB, %s OBP, %s OPS \n #homerun #fantasybaseball' % (p_name, team, av, rstat, hr, rbi, sb, obp, OPS)


        auth = tweepy.OAuthHandler(tkey, tsecret)
        auth.set_access_token(atoke, asecret)

        api = tweepy.API(auth)

        api.update_status(tweet)


    # Gathers player stats and constructs tweets for Stolen Bases
    def steal_parse(self, response):

        p_name = response.xpath('//div[@class="player-info-box-name"]/h1/text()').extract_first()
        team = response.xpath('//div[@class="player-info-box-name-team"]/a/text()').extract_first()
        av = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[17]/text()').extract_first()
        rstat = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[7]/text()').extract_first()
        hr = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[6]/text()').extract_first()
        rbi = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[8]/text()').extract_first()
        sb = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[9]/text()').extract_first()
        obp = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[18]/text()').extract_first()
        slg = response.xpath('//table[@class="rgMasterTable"][1]/tbody/tr[2]/td[19]/text()').extract_first()
        OPS = str(float(obp)+float(slg))

        # Composes and Posts Tweet
        tweet = u'\U0001F3C3' + ' Stolen Base! ' + u'\U0001F3C3' + '- %s - %s \n Season Totals \n %s AVG, %s R, %s HR, %s RBI, %s SB, %s OBP, %s OPS \n #bags #fantasybaseball' % (p_name, team, av, rstat, hr, rbi, sb, obp, OPS)
        auth = tweepy.OAuthHandler(tkey, tsecret)
        auth.set_access_token(atoke, asecret)

        api = tweepy.API(auth)

        api.update_status(tweet)

    pass

# Allows spider to be run by calling file instead of using scrapy commands
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': "@RotoRise Podcast's Friendly Neighborhood Spider",
        'HTTPCACHE_ENABLED': False,
    })

    process.crawl(FanGraphSpider)
    process.start()




