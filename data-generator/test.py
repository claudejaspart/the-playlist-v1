from urllib.parse import quote, unquote
test=quote("Hello I'm very / happy to see you !!!")
print(unquote(test))