domain = 'giaoduc.net.vn'
url = 'https://giaoduc.net.vn/'

protocol1 = 'https://'
protocol2 = 'http://'

print(url.find(protocol1 + domain) >= 0 and len(url) == len(protocol1 + domain))
print(url.find(protocol1 + domain + '/') >= 0 and len(url) == len(protocol1 + domain + '/'))
print(url.find(protocol2 + domain) >= 0 and len(url) == len(protocol2 + domain))
print(url.find(protocol2 + domain + '/') >= 0 and len(url) == len(protocol2 + domain + '/'))

# - 1