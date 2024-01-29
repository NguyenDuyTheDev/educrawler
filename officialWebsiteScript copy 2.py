from educrawler.spiders.websiteCrawler import WebsiteCrawler
from educrawler.spiders.domainCrawler import DomainCrawler
from educrawler.spiders.webpageCrawler import WebpageCrawler
from educrawler.spiders.officialWebsiteCrawler import OfficialWebsiteCrawler
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

class Crawler:
  def __init__(
    self, 
    links = [],
    file_format = [],
    keyword = [],
    rules = [],
    type = "webpage",
    download_delay = 2,
    depth_limit = 3, 
    concurrent_pdomain = 8, 
    concurrent_pid = 0, 
  ) -> None:  
    self.type   = type
    self.spider = None
    self.settings = {}
    self.settings["LINKS"]                          = links
    self.settings["ALLOWED_FILE_FORMAT"]            = file_format
    self.settings["ALLOWED_KEYWORD"]                = keyword
    self.settings["DOWNLOAD_DELAY"]                 = download_delay
    self.settings["DEPTH_LIMIT"]                    = depth_limit
    self.settings["CONCURRENT_REQUESTS_PER_DOMAIN"] = concurrent_pdomain
    self.settings["CONCURRENT_REQUESTS_PER_IP"]     = concurrent_pid
    self.settings["CUSTOM_CRAWL"]                   = []
    self.custom_crawl = rules

    # Custom Rules: (tag_name, class or id, name, inner)
    
  def init_spider(self):
    if self.spider != None:
      return
        
    spider_default_setting = get_project_settings()
    if self.type == "webpage":
      self.spider = CrawlerProcess(settings=spider_default_setting)
      self.spider.crawl(WebpageCrawler, user_settings=self.settings, custom_crawl_rules=self.custom_crawl)

    if self.type == "website":
      self.spider = CrawlerProcess(settings=spider_default_setting)
      #self.spider.crawl(OfficialWebsiteCrawler, user_settings=self.settings)
      self.spider.crawl(OfficialWebsiteCrawler, user_settings=self.settings, custom_crawl_rules=self.custom_crawl)


  def get_settings(self):
    return self.settings

  def is_init(self):
    return self.spider != None

  def start_crawling(self):
    if self.spider == None:
      return
    
    self.spider.start()
    self.spider.stop()
    print('Crawl Completed')
    
def main():
  #url = "https://giaoduc.net.vn/giao-duc-vi-nhan-sinh-hay-vi-hoc-thuat-post239150.gd"
  url = "https://moet.gov.vn/tintuc/Pages/tin-tong-hop.aspx"

  user_settings = {}
  user_settings["LINKS"]                          = [url] 
  user_settings["ALLOWED_FILE_FORMAT"]            = ['png','jpeg','jpg'] 
  user_settings["ALLOWED_KEYWORD"]                = [] 
  user_settings["DOWNLOAD_DELAY"]                 = 2
  user_settings["DEPTH_LIMIT"]                    = 3
  user_settings["CONCURRENT_REQUESTS_PER_DOMAIN"] = 8
  user_settings["CONCURRENT_REQUESTS_PER_IP"]     = 0
  rules = []
  rules.append(("p", None, None, None, None))
  rules.append(("a", None, None, "href", None))
  rules.append(("img", None, None, "src", None))
  rules.append(("video", None, None, "src", None))
  
  crawler = Crawler(
    links             =user_settings["LINKS"],
    file_format       =user_settings["ALLOWED_FILE_FORMAT"],
    keyword           =user_settings["ALLOWED_KEYWORD"],
    rules             =rules,
    type              ="website",
    download_delay    =user_settings["DOWNLOAD_DELAY"],
    depth_limit       =user_settings["DEPTH_LIMIT"],
    concurrent_pdomain=user_settings["CONCURRENT_REQUESTS_PER_DOMAIN"],
    concurrent_pid    =user_settings["CONCURRENT_REQUESTS_PER_IP"]
  )
  crawler.init_spider()
  crawler.start_crawling()    
  
if __name__ == '__main__':
  main()