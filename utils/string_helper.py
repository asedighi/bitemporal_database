
import string
import random

start = 0


def get_random_value() -> str:
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(8))

    return 'Value_'+result_str

def remove_underscore(input: str) -> int:
    return int(input.replace("_", ""))



def get_next_value() -> str:
    global start
    start += 1
    v = str(start)
    return 'Value_'+v


def get_next_int() -> str:
    global start
    start += 1
    return str(start)


