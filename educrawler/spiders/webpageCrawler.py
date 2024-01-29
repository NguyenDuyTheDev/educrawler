from typing import Any, Optional
import scrapy
import logging
from scrapy.http import Response
from scrapy.utils.log import configure_logging
from urllib.parse import urlparse, urljoin
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from educrawler.utils import CssSelectorGenerator, CSSAttributeType, CSSContentType, countExistedTimes, removeEmptySpaceParagraph, removeHTMLTag, removeEmptyLine, countLetterInParagraph, countExistedTimesTokenize
import math

class WebpageCrawler(scrapy.Spider):
  name = "webpageCrawler"
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

  supported_file_format = ["mpg","mpeg","avi","wmv","mov","rm","ram","swf","flv","ogg","webm","mp4","mid","mid","wma","aac","wav","mp3","pdf","epub","doc","docx","png","jpg","jpeg","gif","tiff","tif"]
  allowed_file_format = []
  allowed_keyword = []

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
    super(WebpageCrawler, self).__init__(name, **kwargs)

    if len(user_settings["ALLOWED_FILE_FORMAT"]) == 0:
      self.allowed_file_format = self.supported_file_format
    else:
      self.allowed_file_format = user_settings["ALLOWED_FILE_FORMAT"]
    self.allowed_keyword = user_settings["ALLOWED_KEYWORD"]
    if (len(self.allowed_keyword) == 0):
      self.allowed_keyword = self.academic_keyword

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
    converted_headers = self.convert(response.headers)
    
    if ("text/html" in converted_headers["Content-Type"]):
      academic_keywords = 0
    
      # Crawl Basic Data
    
      '''
      websiteTitle = response.css('title::text').get()
      content   = response.css('p').getall()
      rawHrefs  = response.css('a::attr(href)').getall()
      images    = response.css('img::attr(src)').getall()            
      rawFiles  = response.css('video::attr(src)').getall()
      rawFiles.append(response.css('source::attr(src)').getall())
      rawFiles.append(response.css('audio::attr(src)').getall())
      '''
      websiteTitle = ""
      content = []
      rawHrefs = []
      images    = []        
      rawFiles = []
    
      # Custom Content
      for customRule in self.custom_crawl_rules:
        rule = CssSelectorGenerator(customRule)
        rawContent = response.css(rule).getall()
        if len(rawContent) == 0:
          continue
              
        contentAttr = CSSAttributeType(customRule)
        contentType = CSSContentType(customRule)        
                      
        if contentType == "a":
          rawHrefs += rawContent
   
        elif contentType == "img":
          images += rawContent
                
        elif contentType == "video":
          rawFiles += rawContent
              
        elif contentType == "source":
          rawFiles += rawContent

        elif contentType == "audio":
          rawFiles += rawContent
              
        else:
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
        for fileFormat in self.allowed_file_format:
          if fileFormat.lower() in recentSrc.lower():
            realFiles.append(recentSrc)
            break
        realFiles.append(recentSrc)
    
      # Extract absolute img url from a tag
      for href in realHrefs:
        for fileFormat in self.allowed_file_format:
          dotFileFormat = ".".join(fileFormat)
          if dotFileFormat in href.lower():
            clean_images.append(href)
            break
            
      # Extract absolute files url from a tag
      for href in realHrefs:
        for fileFormat in self.allowed_file_format:
          dotFileFormat = ".".join(fileFormat)
          if dotFileFormat in href.lower():
            realFiles.append(href)
            break
            
      realFiles.append(response.url)
            
      found_keywords = []
      raw_content = "\n".join(content)
      raw_content = removeHTMLTag(raw_content)
      raw_content = removeEmptyLine(raw_content)
      raw_content = removeEmptySpaceParagraph(raw_content)
      total_words = countLetterInParagraph(raw_content)
      minimum_keywords = math.floor(total_words * 1.0 / 200)
      
      # Check if academic content      
      for keyword in self.allowed_keyword:
        count = countExistedTimesTokenize(websiteTitle, keyword)
        if count > 0:
          found_keywords.append(keyword)
          academic_keywords += count
      
        count = countExistedTimesTokenize(raw_content, keyword)
        if count > minimum_keywords:
          found_keywords.append(keyword)
          academic_keywords += count
    
      if len(self.allowed_keyword) == 0 or (len(self.allowed_keyword) > 0 and academic_keywords > 0):
        items = {
          "phase": "webpage",
          "item_type": "HTML",
          "domain": self.allowed_domains[0],
          "url": response.url,
          "academic_keyword": academic_keywords,
          "keywords": found_keywords,
          "h1": "",
          "h2": "",
          "h3": "",
          "h4": "",
          "h5": "",
          "h6": "",
          "title": websiteTitle,
          "content" : content,
          "reformatted_content": raw_content,
          "total_words": total_words,
          "minimum_keywords": minimum_keywords,
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
      for fileFormat in self.allowed_file_format:
        dotFileFormat = ".".join(fileFormat)
        if dotFileFormat in response.url.lower():
          isSuitableFile = True  
          break
            
      # Extract absolute files url from a tag
      for fileFormat in self.allowed_file_format:
        dotFileFormat = ".".join(fileFormat)
        if dotFileFormat in response.url.lower():
          isSuitableFile = True  
          break
          
      if isSuitableFile:
        yield {
          "phase": "file",
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