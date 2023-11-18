def CssSelectorGenerator(basicCss):  
  result = ""
  recentCss = basicCss
  
  while recentCss != None:
    result += recentCss[0]
    if recentCss[1] == "class":
      result += "." + recentCss[2]
    if recentCss[1] == "id":
      result += "#" + recentCss[2]   
      
    if recentCss[3] == "text":
      result += "::text"
    if recentCss[3] == "href":
      result += "::attr(href)"
    if recentCss[3] == "src":
      result += "::attr(src)"      
    
    result += " "
      
    recentCss = recentCss[4]
    
  return result

def CSSAttributeType(basicCss):
  recentCss = basicCss
  while recentCss != None:
    if recentCss[3] == "text":
      return "text"
    if recentCss[3] == "href":
      return "href"
    if recentCss[3] == "src":
      return "src"            
    recentCss = recentCss[4]
  return "none"

def CSSContentType(basicCss):
  if basicCss == None:
    return "none"
  recentCss = basicCss
  while recentCss[4] != None:
    recentCss = recentCss[4]
  return recentCss[0]