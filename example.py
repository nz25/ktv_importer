example = 'café'

for c in example:
    print(c, ord(c))

print(example)

print(len(example))

bytes_utf8 = bytes(example, 'utf-8')

print(bytes_utf8)

print(len(bytes_utf8))

bytes_utf16 = bytes(example, 'utf-16')

print(bytes_utf16)

print(len(bytes_utf16))

ex_utf8 = str(bytes_utf8, 'utf-8')
print(ex_utf8)

ex_utf16 = str(bytes_utf16, 'ascii')
print(ex_utf16)