# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline
from educrawler.utils import removeHTMLTag, removeEmptyLine

import psycopg2
from datetime import datetime

class EducrawlerPipeline:
    def process_item(self, item, spider):
        return item

class CustomImagesPipeline(ImagesPipeline):
  def file_path(self, request, response=None, info=None, *, item=None):
    reformated_url = request.url.split('/')[-1]
    if "?" in reformated_url:
      reformated_url = reformated_url.split('?')[0]
    return reformated_url
  
class CustomFilesPipeline(FilesPipeline):
  def file_path(self, request, response=None, info=None, *, item=None):
    reformated_url = request.url.split('/')[-1]
    if "?" in reformated_url:
      reformated_url = reformated_url.split('?')[0]
    return reformated_url
  
class PostgresPipeline:
  def __init__(self):
  ## Connection Details
    hostname = 'localhost'
    username = 'postgres'
    password = 'k0K0R0som@lI' # your password
    database = 'FinalProject'

    ## Create/Connect to database
    self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
    ## Create cursor, used to execute commands
    self.cur = self.connection.cursor()
    
    print('initial spider')
      
  def open_spider(self, spider):
    for url in spider.allowed_domains:
      sql_check_command = '''SELECT * FROM "FinalProject"."Domain" WHERE "Url" LIKE '%''' + url + '''%' '''
      self.cur.execute(sql_check_command)
      result = self.cur.fetchone()
      if result:
        print(result[0])
        now = datetime.utcnow()
        sql_update_command = '''
          update "FinalProject"."Domain" 
          set "StartTime" = TIMESTAMP '%s',
              "IsCrawling" = TRUE
          where "ID" = %s
          ''' % (str(now), result[0])  
        self.cur.execute(sql_update_command)
        self.connection.commit()
        
    print('open spider')
          
  def close_spider(self, spider):
    ## Update End Time
    for url in spider.allowed_domains:
      sql_check_command = '''SELECT * FROM "FinalProject"."Domain" WHERE "Url" LIKE '%''' + url + '''%' '''
      self.cur.execute(sql_check_command)
      self.connection.commit()
      result = self.cur.fetchone()
      if result:
        print(result[0])
        now = datetime.utcnow()
        sql_update_command = '''
          update "FinalProject"."Domain" 
          set "EndTime" = TIMESTAMP '%s',
              "IsCrawling" = FALSE
          where "ID" = %s
          ''' % (str(now), result[0])  
        self.cur.execute(sql_update_command)
        self.connection.commit()

    ## Close cursor & connection to database 
    self.cur.close()
    self.connection.close()
    
    print('close spider')
      
  def process_item(self, item, spider):
    if item["item_type"] == "File":
      return item
    
    sql_check_command = '''
    select * from "FinalProject"."RawData" where "Url" = '%s'
    ''' % (item["url"])
    self.cur.execute(sql_check_command)
    result = self.cur.fetchone()
    
    # Process Data
    h1 = "" 
    h2 = ""
    h3 = ""
    h4 = ""
    h5 = ""
    h6 = ""
    title = ""
          
    if item["h1"] is not None: 
      h1 = item["h1"].replace("'", "").replace('"', '').replace('\n','')
    if item["h2"] is not None: 
      h2 = item["h2"].replace("'", "").replace('"', '').replace('\n','')
    if item["h3"] is not None: 
      h3 = item["h3"].replace("'", "").replace('"', '').replace('\n','')
    if item["h4"] is not None: 
      h4 = item["h4"].replace("'", "").replace('"', '').replace('\n','')
    if item["h5"] is not None: 
      h5 = item["h5"].replace("'", "").replace('"', '').replace('\n','')
    if item["h6"] is not None: 
      h6 = item["h6"].replace("'", "").replace('"', '').replace('\n','')
    if item["title"] is not None: 
      title = item["title"].replace("'", "").replace('"', '')
    
    reformated_content = []
    for raw_content in item["content"]:    
      reformated_content_unit = raw_content
      #reformated_content_unit = reformated_content_unit.replace("'", "").replace('"', '')
                        
      while reformated_content_unit.find("<p") != -1:
        idx1 = reformated_content_unit.find("<p")
        idx2 = reformated_content_unit.find(">")
        res = reformated_content_unit[idx1 + len("<p") + 1: idx2]
        reformated_content_unit = reformated_content_unit.replace("<p " + res + ">", "")
        reformated_content_unit = reformated_content_unit.replace("<p>", "")
        reformated_content_unit = reformated_content_unit.replace("</p>", "")
                 
      while reformated_content_unit.find("<a") != -1:
        idx1 = reformated_content_unit.find("<a")
        idx2 = reformated_content_unit.find(">", idx1)
        res = reformated_content_unit[idx1 + len("<a") + 1: idx2]
        reformated_content_unit = reformated_content_unit.replace("<a " + res + ">", "")
        reformated_content_unit = reformated_content_unit.replace("<a>", "")
        reformated_content_unit = reformated_content_unit.replace("</a>", "")
            
      while reformated_content_unit.find("<strong") != -1:
        idx1 = reformated_content_unit.find("<strong")
        idx2 = reformated_content_unit.find(">")
        res = reformated_content_unit[idx1 + len("<strong") + 1: idx2]
        reformated_content_unit = reformated_content_unit.replace("<strong " + res + ">", "")
        reformated_content_unit = reformated_content_unit.replace("<strong>", "")
        reformated_content_unit = reformated_content_unit.replace("</strong>", "")
 
      while reformated_content_unit.find("<em") != -1:       
        idx1 = reformated_content_unit.find("<em")
        idx2 = reformated_content_unit.find(">")
        res = reformated_content_unit[idx1 + len("<em") + 1: idx2]
        reformated_content_unit = reformated_content_unit.replace("<em " + res + ">", "")
        reformated_content_unit = reformated_content_unit.replace("<em>", "")
        reformated_content_unit = reformated_content_unit.replace("</em>", "")
       
      '''       
      reformated_content_unit = reformated_content_unit.replace("<p>", "")
      reformated_content_unit = reformated_content_unit.replace("</p>", "")
      reformated_content_unit = reformated_content_unit.replace("<a>", "")
      reformated_content_unit = reformated_content_unit.replace("</a>", "")
      reformated_content_unit = reformated_content_unit.replace("<strong>", "")
      reformated_content_unit = reformated_content_unit.replace("</strong>", "")
      reformated_content_unit = reformated_content_unit.replace("<em>", "")
      reformated_content_unit = reformated_content_unit.replace("</em>", "")
      '''
      
      #reformated_content_unit = reformated_content_unit.replace("\"", "")
      reformated_content.append(reformated_content_unit)
    
    sql_insert_content = 'ARRAY[]::Text[] []'
    if len(reformated_content) > 0:
      sql_insert_content = 'ARRAY %s' % str(reformated_content)
    
    sql_insert_img = 'ARRAY[]::Text[] []'
    if len(item["image_urls"]) > 0:
      sql_insert_img = 'ARRAY %s' % str(item["image_urls"])
            
    sql_insert_file = 'ARRAY[]::Text[] []'
    if len(item["file_urls"]) > 0:
      sql_insert_file = 'ARRAY %s' % str(item["file_urls"])
    
    if result:          
      sql_delete_command = '''
      DELETE FROM "FinalProject"."News" 
      WHERE "url" = '%s';
      ''' % item["url"]
    
      try:
        self.cur.execute(sql_delete_command)
        self.connection.commit()
      except Exception as error:
        spider.logger.warn("Oops! An exception has occured:", error)
        spider.logger.warn("Exception TYPE:", type(error))
        spider.logger.warn("Error in sql delete: " + sql_delete_command)
        self.cur.close()
        self.connection.close()
        self.__init__()     
        
    sql_command = '''
    insert into "FinalProject"."News" 
    ("title", "url", "content", "source") 
    values (
      '%s',
      '%s',
      %s,
      '%s'
    )
    ''' % (title, item["url"], sql_insert_content, item["domain"])
         
    try:
      self.cur.execute(sql_command)
      self.connection.commit()
    except Exception as error:
      spider.logger.warn("Oops! An exception has occured:", error)
      spider.logger.warn("Exception TYPE:", type(error))
      spider.logger.warn("Error in sql insert: " + sql_command)
      self.cur.close()
      self.connection.close()
      self.__init__() 
              
    return item
  
  
class WebpagePipeLine:
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
  
  def __init__(self):
  ## Connection Details
    hostname = 'localhost'
    username = 'postgres'
    password = 'k0K0R0som@lI' # your password
    database = 'FinalProject'

    ## Create/Connect to database
    self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
    ## Create cursor, used to execute commands
    self.cur = self.connection.cursor()
    
    print('initial spider')
      
  def open_spider(self, spider):
    print('open spider')
          
  def close_spider(self, spider):
    ## Close cursor & connection to database 
    self.cur.close()
    self.connection.close()
    
    print('close spider')
      
  def process_item(self, item, spider):
    if item["phase"] != "webpage":
      return item
    
    sql_check_command = '''
    select * from "FinalProject"."Article" where "Url" = '%s'
    ''' % (item["url"])
    self.cur.execute(sql_check_command)
    result = self.cur.fetchone()
    
    # Process Data
    title = ""
          
    if item["title"] is not None: 
      title = item["title"].replace("'", "").replace('"', '')
    
    reformated_content = []
    for raw_content in item["content"]:    
      reformated_content_unit = removeHTMLTag(raw_content)
      
      if len(reformated_content_unit) > 0:
        reformated_content.append(reformated_content_unit)
          
    reformated_files_name = []
    for raw_file_name in item["image_urls"]:    
      reformated_files_name.append(raw_file_name.split('/')[-1])

    reformated_content_as_string = "\n".join(reformated_content)
    reformated_content_as_string = removeEmptyLine(reformated_content_as_string)
    
    imgs_as_tring = ",".join(reformated_files_name)
    files_as_tring = ",".join(item["file_urls"])
    crawl_status = "Good"
    note = ""
    
    # Check
    tokens = reformated_content_as_string.split(" ") 
    total_all_word = 0
    for keyword in self.academic_keyword:
      total_keyword = 0
      
      for token in tokens:
        if keyword in token:
          total_keyword += 1
        
      if total_keyword > int(len(tokens) / 200):
        total_all_word += 1
    
    if result and total_all_word >= 0:          
      sql_delete_command = '''
      DELETE FROM "FinalProject"."Article" 
      WHERE "Url" = '%s';
      ''' % item["url"]
    
      try:
        self.cur.execute(sql_delete_command)
        self.connection.commit()
      except Exception as error:
        spider.logger.warn("Oops! An exception has occured:", error)
        spider.logger.warn("Exception TYPE:", type(error))
        spider.logger.warn("Error in sql delete: " + sql_delete_command)
        self.cur.close()
        self.connection.close()
        self.__init__()     
        
    if total_all_word > 0:
      sql_command = '''
      insert into "FinalProject"."Article" 
      ("Domain", "Url", "FileName", "Content", "Images", "Files", "CrawlStatus", "Note") 
      values (
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s',
        '%s'
      )
      ''' % (item["domain"], item["url"], item["url"], reformated_content_as_string, imgs_as_tring, files_as_tring, crawl_status, note)
          
      try:
        self.cur.execute(sql_command)
        self.connection.commit()
      except Exception as error:
        spider.logger.warn("Oops! An exception has occured:", error)
        spider.logger.warn("Exception TYPE:", type(error))
        spider.logger.warn("Error in sql insert: " + sql_command)
        self.cur.close()
        self.connection.close()
        self.__init__() 
              
    return item
  