serial = 50
variable = 'f1l[{c_3}].f1'
answer = 'don\'t know'
answer = 'axa\naxa'

print(','.join(str(x) for x in (serial, variable, answer)))
print((serial, variable, answer))

def build_values(serial, variable, answer):
    answer = answer.replace("'", "''")

    values = '(' + str(serial) + ", '" + variable + "', '" + answer + "')"
    return values


print(build_values(serial, variable, answer))