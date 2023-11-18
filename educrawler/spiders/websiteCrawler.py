from typing import Any, Optional
import scrapy
import logging
from scrapy.http import Response
from scrapy.utils.log import configure_logging
from urllib.parse import urlparse, urljoin
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from educrawler.utils import CssSelectorGenerator, CSSAttributeType, CSSContentType

class WebsiteCrawler(scrapy.Spider):
  name = "websiteCrawler"
  allowed_domains = []
  start_urls = []
  visited = []
  upcomming = []
  image_formats = ["png","jpg","jpeg","gif","tiff","tif"]
  multimedia_format = [
    "mpg","mpeg","avi","wmv","mov","rm","ram","swf","flv","ogg","webm","mp4", # video
    "mid","mid","wma","aac","wav","mp3", # audio
    "pdf","epub","doc","docx","svg" # document
  ]

  academic_keyword = [
    "giáo dục",
    "đại học",
    "trường",
    "học",
    "dạy",
    "phổ thông",
    "tiểu học",
    "mầm non",
    "giáo viên",
    "đào tạo",
    "nghề",
    "sinh viên",
    "học sinh",
    "ngành",
    "khoa",
    "trung học",
    "môn",
    "ngữ văn",
    "bài tập",
    "chuyên",
    "học đường",
    "trải nghiệm",
    "vận động",
    "kĩ năng",
    "kiến trúc",
    "học tập",
    "kĩ năng mềm",
    "rèn luyện",
    "giảng viên",
    "sư phạm",
    "tư duy",
    "phân tích",
    "thực nghiệm",
    "giải quyết vấn đề",
    "toán",
    "tự học",
    "hướng dẫn",
    "đánh giá",
    "kết quả",
  ]
  
  uncrawlable_link = [
    "mailto", "javascript", "commentbox", "tel"
  ] 
  
  custom_settings = {
    'FEEDS': { 
      'crawledData/%(name)s/%(name)s_batch_%(batch_id)d.json': { 
        'format': 'json', 
        'encoding': 'utf-8',
        'batch_item_count': 20,
        'indent': 4,
        'item_export_kwargs': {
          'export_empty_fields': True,
        },
        'overwrite': True
        }
    }
  }
  
  configure_logging(install_root_handler=False)
  logging.basicConfig(
    filename='log.txt',
    format='%(levelname)s: %(message)s',
    level=logging.INFO,
    encoding='utf8'
  )
  
  def __init__(self, user_settings, custom_crawl_rules = [], name: str | None = None, **kwargs: Any):
    super(WebsiteCrawler, self).__init__(name, **kwargs)

    for link in user_settings["LINKS"]:
      self.start_urls.append(link)
        
    for url in self.start_urls:
      parsed_uri = urlparse(url)
      domain = '{uri.netloc}'.format(uri=parsed_uri)
      self.allowed_domains.append(domain)
          
    self.download_delay                                     = user_settings["DOWNLOAD_DELAY"]
    self.custom_settings["DEPTH_LIMIT"]                     = user_settings["DEPTH_LIMIT"]
    self.custom_settings["CONCURRENT_REQUESTS_PER_DOMAIN"]  = user_settings["CONCURRENT_REQUESTS_PER_DOMAIN"]
    self.custom_settings["CONCURRENT_REQUESTS_PER_IP"]      = user_settings["CONCURRENT_REQUESTS_PER_IP"]
        
    self.custom_crawl_rules = custom_crawl_rules
    
  def start_requests(self):
    for url in self.start_urls:
      yield scrapy.Request(
        url=url, 
        callback=self.parse,
        errback=self.errback_httpbin,
      )
      
  def parse(self, response):
    if response.url in self.upcomming:
      self.upcomming.remove(response.url)
    self.visited.append(response.url)
    
    converted_headers = self.convert(response.headers)
    
    if ("text/html" in converted_headers["Content-Type"]):
      academic_keywords = 0
    
      # Crawl Basic Data
      title1 = response.css('h1::text').get()
      title2 = response.css('h2::text').get()
      title3 = response.css('h3::text').get()
      title4 = response.css('h4::text').get()
      title5 = response.css('h5::text').get()
      title6 = response.css('h6::text').get()
      websiteTitle = response.css('title::text').get()
    
      content   = response.css('p').getall()
      rawHrefs  = response.css('a::attr(href)').getall()
      images    = response.css('img::attr(src)').getall()            
      rawFiles  = response.css('video::attr(src)').getall()
      rawFiles.append(response.css('source::attr(src)').getall())
      rawFiles.append(response.css('audio::attr(src)').getall())
    
      # Custom Content
      for customRule in self.custom_crawl_rules:
        rule = CssSelectorGenerator(customRule)
        rawContent = response.css(rule).getall()
        if len(rawContent) == 0:
          continue
              
        contentAttr = CSSAttributeType(customRule)
        contentType = CSSContentType(customRule)        
                      
        if contentType == "a" and contentAttr == "href":
          rawHrefs += rawContent
                
        if contentType == "video" and contentAttr == "src":
          rawFiles += rawContent
              
        if contentType == "source" and contentAttr == "src":
          rawFiles += rawContent

        if contentType == "audio" and contentAttr == "src":
          rawFiles += rawContent
              
        if contentAttr == "none" or contentAttr == "text": 
          content += rawContent
    
      # Cleaning Href 
      realHrefs = []
      for href in rawHrefs:
        # Remove empty href
        if len(href) == 0:
          continue  
        if href == "#":
          continue
        if href == "/":
          continue
              
        # is real href
        isLink = True
        for tag in self.uncrawlable_link:
          if tag in href:
            isLink = False
            break
          if isLink == False:
            continue
              
        # Reformat link
        parsedHref = urlparse(href)
        recentHref = href
        if recentHref[0] == "/":
          recentHref = urljoin(response.url, href)
          parsedHref = urlparse(recentHref)
                
        # Check after append
        if not parsedHref.scheme:
          continue
        if "http" not in parsedHref.scheme:
          continue
              
        # Check if same domain 
        if not bool(parsedHref.netloc):
          recentHref = urljoin(response.url, recentHref)
        realHrefs.append(recentHref)
    
      # Extract absolute img url for img tag
      clean_images = []
      for raw_image_url in images:
        recentLink = raw_image_url
        parsed_raw_image_url = urlparse(raw_image_url)
        if len(recentLink) == 0:
          continue
        if recentLink[0] == "/":
          recentLink = urljoin(response.url, raw_image_url)
          parsed_raw_image_url = urlparse(recentLink)
        if not parsed_raw_image_url.scheme:
          continue
        if "http" not in parsed_raw_image_url.scheme:
          continue
        if ".svg" in recentLink.lower():
          continue
        clean_images.append(recentLink)
    
      # Extract absolute files url from multimedia tag
      realFiles = []
      for src in rawFiles:
        # Remove empty href
        if len(src) == 0:
          continue 
        if src == "#":
          continue
        if src == "/":
          continue
              
        # Format link
        parsedSrc = urlparse(src)
        recentSrc = src
        if recentSrc[0] == "/":
          recentSrc = urljoin(response.url, src)
          parsedSrc = urlparse(parsedSrc)
                
        # Check after append
        if not parsedSrc.scheme:
          continue
        if "http" not in parsedSrc.scheme:
          continue
              
        # Check if same domain 
        if not bool(parsedSrc.netloc):
          recentSrc = urljoin(response.url, recentSrc)
        for fileFormat in self.multimedia_format:
          if fileFormat.lower() in recentSrc.lower():
            realFiles.append(recentSrc)
            break
        realFiles.append(recentSrc)
    
      # Extract absolute img url from a tag
      for href in realHrefs:
        for fileFormat in self.image_formats:
          dotFileFormat = ".".join(fileFormat)
          if dotFileFormat in href.lower():
            clean_images.append(href)
            break
            
      # Extract absolute files url from a tag
      for href in realHrefs:
        for fileFormat in self.multimedia_format:
          dotFileFormat = ".".join(fileFormat)
          if dotFileFormat in href.lower():
            realFiles.append(href)
            break
            
      realFiles.append(response.url)
            
      # Check if academic content
      for keyword in self.academic_keyword:
        if keyword in websiteTitle:
          academic_keywords += 1
        for content_row in content:
          if keyword in content_row:
            academic_keywords += 1
    
      items = {
        "item_type": "HTML",
        "url": response.url,
        "academic_keyword": academic_keywords,
        "h1": title1,
        "h2": title2,
        "h3": title3,
        "h4": title4,
        "h5": title5,
        "h6": title6,
        "title": websiteTitle,
        "content" : content,
        "link": realHrefs,
        "image_urls": clean_images,
        "file_urls": realFiles
      }
      yield items
    
    # Find next link 
    if ("text/html" in converted_headers["Content-Type"]):
      return
    else:
      isSuitableFile = False
      # Extract absolute img url from a tag
      for fileFormat in self.image_formats:
        dotFileFormat = ".".join(fileFormat)
        if dotFileFormat in response.url.lower():
          isSuitableFile = True  
          break
            
      # Extract absolute files url from a tag
      for fileFormat in self.multimedia_format:
        dotFileFormat = ".".join(fileFormat)
        if dotFileFormat in response.url.lower():
          isSuitableFile = True  
          break
          
      if isSuitableFile:
        yield {
          "item_type": "File",
          "domain": "",
          "url": response.url,
          "academic_keyword": "",
          "h1": "",
          "h2": "",
          "h3": "",
          "h4": "",
          "h5": "",
          "h6": "",
          "title": "",
          "content" : [],
          "link": [],
          "image_urls": [],
          "file_urls": [response.url]
        }         
    
  def errback_httpbin(self, failure):
    # log all failures
    self.logger.error(repr(failure))

    if failure.check(HttpError):
      # these exceptions come from HttpError spider middleware
      # you can get the non-200 response
      response = failure.value.response
      self.logger.error("HttpError on %s", response.url)

    elif failure.check(DNSLookupError):
      # this is the original request
      request = failure.request
      self.logger.error("DNSLookupError on %s", request.url)

    elif failure.check(TimeoutError, TCPTimedOutError):
      request = failure.request
      self.logger.error("TimeoutError on %s", request.url)
    
  def convert(self, data):
    if isinstance(data, bytes):  return data.decode('ascii')
    if isinstance(data, list):   return data.pop().decode('ascii')
    if isinstance(data, dict):   return dict(map(self.convert, data.items()))
    if isinstance(data, tuple):  return map(self.convert, data)
    return data