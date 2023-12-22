from prompt_toolkit import prompt
from parse import parse

while True:
    user_input = prompt("QS_MyDBSystem ~ ")
    if (user_input == "exit"):
        break
        
    output = parse(user_input)
    if isinstance(output, (list, dict)):
        for i in output:
            print(i)
    else:
        print(output)