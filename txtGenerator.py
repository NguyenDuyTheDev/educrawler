import io
import psycopg2

hostname = 'localhost'
username = 'postgres'
password = 'k0K0R0som@lI' # your password
database = 'FinalProject'

## Create/Connect to database
connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
# Create cursor, used to execute commands
cur = connection.cursor()

sql_check_command = '''SELECT * FROM "FinalProject"."Article" where "Domain" LIKE 'vnuhcm.edu.vn' '''
cur.execute(sql_check_command)

result = cur.fetchone()

while result:
  filename = result[3].split('/')[-1]
  if (len(filename) == 0):
    filename = result[3].split('/')[-2]
  if "." in filename:
    filename = filename.split('.')[0]
  if "?" in filename:
    filename = filename.split('?')[0]
      
  exportedFileName = "txtFile/" + result[1].split(".")[0] + "/" + filename + ".txt" 

  # exportedFileName = "txtFile/" + filename + ".txt" 

  # Clean Content

  content = result[4]
  error1 = "CƠ QUAN CỦA BỘ GIÁO DỤC VÀ ĐÀO TẠO - DIỄN ĐÀN TOÀN XÃ HỘI VÌ SỰ NGHIỆP GIÁO DỤC"
  error2 = "Bình luận của bạn đã được gửi và sẽ hiển thị sau khi được duyệt bởi ban biên tập."
  error3 = "Tổng số điểm của bài viết là"
  index1 = content.find(error1)
  if index1 >= 0:
    content = content[:index1]
  index1 = content.find(error2)
  if index1 >= 0:
    content = content[:index1]
  index1 = content.find(error3)
  if index1 >= 0:
    content = content[:index1]
    
  while content.find("<img") != -1:       
    idx1 = content.find("<img")
    idx2 = content.find(">", idx1)
    res = content[idx1 + len("<img") + 1: idx2]
    content = content.replace("<img " + res + ">", "") 
  
  #while content.find("<i") != -1:       
  #  idx1 = content.find("<i")
  #  idx2 = content.find("/i>", idx1)
  #  res = content[idx1 + len("<i") + 1: idx2]
  #  content = content.replace("<i " + res + "/i>", "") 
  
  content = content.replace("<b>", "") 
  content = content.replace("</b>", "") 
  
  flag = True
  
  if exportedFileName.find("tag") != -1:
    flag = False
  if len(content) < 100:
    flag = False
  
  if flag:
    try:
      with io.open(exportedFileName,'w',encoding='utf8') as f:
        f.write(content)
    except:
      print("Error: " + exportedFileName)  
    
  result = cur.fetchone()