'''
domain = 'giaoduc.net.vn'
url = 'https://giaoduc.net.vn/'

protocol1 = 'https://'
protocol2 = 'http://'

print(url.find(protocol1 + domain) >= 0 and len(url) == len(protocol1 + domain))
print(url.find(protocol1 + domain + '/') >= 0 and len(url) == len(protocol1 + domain + '/'))
print(url.find(protocol2 + domain) >= 0 and len(url) == len(protocol2 + domain))
print(url.find(protocol2 + domain + '/') >= 0 and len(url) == len(protocol2 + domain + '/'))

# - 1
'''
def countExistedTimes(text : str, word : str) -> int:
  text = text.lower()
  word = word.lower()
  count = 0
  index = 0
  while index != -1:
    index = text.find(word, index + 1)
    count += 1
  
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

def countExistedTimesTokenize(text : str, word : str) -> int:
  textAsTokens = text.lower().split(' ')
  wordAsTokens = word.lower().split(' ')
  print(textAsTokens)
  print(wordAsTokens)
  
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

print("Not Tokenize: " + str(countExistedTimes('Trung học cơ sở và trung học phổ thông','Trung học')))
print("Tokenize: " + str(countExistedTimesTokenize('Trung học cơ sở và trung học phổ thông','Trung học')))
print(firstLetterOnly("Trung học  "))