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

def removeHTMLTag(text : str) -> str:
  text = text.replace("<p>", "")
  text = text.replace("</p>", "")
  text = text.replace("<a>", "")
  text = text.replace("</a>", "")
  text = text.replace("<strong>", "")
  text = text.replace("</strong>", "")
  text = text.replace("<em>", "")
  text = text.replace("</em>", "")
  text = text.replace("<span>", "")
  text = text.replace("</span>", "")
  text = text.replace("<font>", "")
  text = text.replace("</font>", "")
  text = text.replace("<br>", "")
  text = text.replace("</i>", "")
  text = text.replace("<i>", "")
  text = text.replace("&nbsp", "")
  text = text.replace("<div>", "")
  text = text.replace("</div>", "")
  
  text = text.replace("<h1>","")
  text = text.replace("</h1>","")
  text = text.replace("<h2>","")
  text = text.replace("</h2>","")
  text = text.replace("<h3>","")
  text = text.replace("</h3>","")
  text = text.replace("<h4>","")
  text = text.replace("</h4>","")
  text = text.replace("<h5>","")
  text = text.replace("</h5>","")
  text = text.replace("<h6>","")
  text = text.replace("</h6>","")

  text = text.replace("<b>","")
  text = text.replace("</b>","")
  text = text.replace("<sub>","")
  text = text.replace("</sub>","")
  text = text.replace("<sup>","")
  text = text.replace("</sup>","")
  text = text.replace("<u>","")
  text = text.replace("</u>","")  
  text = text.replace("<iframe>","")
  text = text.replace("</iframe>","")  
  text = text.replace("<nav>","")
  text = text.replace("</nav>","")  
  text = text.replace("<menu>","")
  text = text.replace("</menu>","")  
  text = text.replace("<ul>","")
  text = text.replace("</ul>","")  
  text = text.replace("<ol>","")
  text = text.replace("</ol>","")  
  text = text.replace("<li>","")
  text = text.replace("</li>","")  
  text = text.replace("<dl>","")
  text = text.replace("</dl>","")  
  text = text.replace("<dt>","")
  text = text.replace("</dt>","")  
  text = text.replace("<dd>","")
  text = text.replace("</dd>","")  
   
  # Table   
  text = text.replace("<table>","")
  text = text.replace("</table>","")  
  text = text.replace("<caption>","")
  text = text.replace("</caption>","")  
  text = text.replace("<th>","")
  text = text.replace("</th>","")  
  text = text.replace("<tr>","")
  text = text.replace("</tr>","")  
  text = text.replace("<td>","")
  text = text.replace("</td>","")  
  text = text.replace("<thead>","")
  text = text.replace("</thead>","")  
  text = text.replace("<tbody>","")
  text = text.replace("</tbody>","")  
  text = text.replace("<tfoot>","")
  text = text.replace("</tfoot>","")  
  text = text.replace("<col>","")
  text = text.replace("</col>","")  
  text = text.replace("<colgroup>","")
  text = text.replace("</colgroup>","")   
      
  # Styles
  text = text.replace("<style>","")
  text = text.replace("</style>","")  
  text = text.replace("<header>","")
  text = text.replace("</header>","")  
  text = text.replace("<hgroup>","")
  text = text.replace("</hgroup>","")  
  text = text.replace("<footer>","")
  text = text.replace("</footer>","")  
  text = text.replace("<main>","")
  text = text.replace("</main>","")  
  text = text.replace("<section>","")
  text = text.replace("</section>","")  
  text = text.replace("<search>","")
  text = text.replace("</search>","")  
  text = text.replace("<article>","")
  text = text.replace("</article>","")  
  text = text.replace("<aside>","")
  text = text.replace("</aside>","")  
  text = text.replace("<details>","")
  text = text.replace("</details>","")  
  text = text.replace("<dialog>","")
  text = text.replace("</dialog>","")  
  text = text.replace("<summary>","")
  text = text.replace("</summary>","")  
  text = text.replace("<data>","")
  text = text.replace("</data>","")  
      
  # Programming
  text = text.replace("<script>","")
  text = text.replace("</script>","")  
  text = text.replace("<noscript>","")
  text = text.replace("</noscript>","")  
      
  while text.find("<div") != -1:
    idx1 = text.find("<div")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")
  
  while text.find("<img") != -1:
    idx1 = text.find("<img")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")
  
  while text.find("<i") != -1:
    idx1 = text.find("<i")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")
  
  while text.find("<p") != -1:
    idx1 = text.find("<p")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")
        
  while text.find("<a") != -1:
    idx1 = text.find("<a")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")

  while text.find("<strong") != -1:
    idx1 = text.find("<strong")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")

  while text.find("<em") != -1:       
    idx1 = text.find("<em")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")

  while text.find("<span") != -1:       
    idx1 = text.find("<span")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")   
      
  while text.find("<font") != -1:       
    idx1 = text.find("<font")
    idx2 = text.find(">", idx1)
    res = text[idx1: idx2 + 1]
    text = text.replace(res, "")
  
  return text

def removeEmptyLine(text : str) -> str:
  rawRow = text.split('\n')
  notEmptyRow = []
  
  for row in rawRow:
    cleanRow = row.strip()
    if len(cleanRow) > 0:
      notEmptyRow.append(cleanRow)
      
  return "\n".join(notEmptyRow)

def countExistedTimes(text : str, word : str) -> int:
  text = text.lower()
  word = word.lower()
  
  count = 0
  index = 0
  while index != -1:
    index = text.find(word, index + 1)
    count += 1
  return count

def countExistedTimesTokenize(text : str, word : str) -> int:
  textAsTokens = text.lower().split(' ')
  wordAsTokens = word.lower().split(' ')
  
  count = 0
  index = 0
  while index < len(textAsTokens):
    if textAsTokens[index] == wordAsTokens[0]:
      if len(wordAsTokens) == 1:
        count += 1
      else:
        remainingWord = len(textAsTokens) - (index) - len(wordAsTokens)
        
        if remainingWord >= 0:
          isSimilar = True
          for subIndex in range(1, len(wordAsTokens)):
            if textAsTokens[index + subIndex] != wordAsTokens[subIndex]:
              isSimilar = False
              break
          if isSimilar == True:
            count += 1
            print(index)
            index += subIndex
    index += 1
  return count

def firstLetterOnly(text: str) -> str:
  words = text.split(' ')
  reformattedText = ""
  
  for word in words:
    removeSpace = word.strip()
    if len(removeSpace) == 0:
      continue
    reformattedText = reformattedText + removeSpace[0]
  
  return reformattedText

def countLetterInParagraph(text: str) -> int:
  rawWords = text.split(' ')
  goodWords = []
  
  for word in rawWords:
    reformattedWord = word.strip()
    if (len(reformattedWord) > 0):
      goodWords.append(reformattedWord)
      
  return len(goodWords)

def removeEmptySpaceParagraph(text: str) -> str:
  rawWords = text.split(' ')
  goodWords = []
  
  for word in rawWords:
    reformattedWord = word.strip()
    if (len(reformattedWord) > 0):
      goodWords.append(reformattedWord)
      
  return " ".join(goodWords)