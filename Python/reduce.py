#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = -1
current_count = ''
word = None
joinKey = None
arrayResult = []
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, type, count = line.split('\t',2)
    # print(word,type,count)

    if current_word == int(word):
        if type == 'movie':
            joinKey = count
        elif joinKey:
            current_count += count + ', ' + joinKey + '\n'
        else:
            arrayResult.append(count)
            # print('------------')
            # print(arrayResult, current_word, count)
    else:
        # print('==================')
        # print(arrayResult, current_word, count, joinKey, type)
        # print(arrayResult, current_word)
        if current_word != -1 and joinKey:
            
            # print('==================')
            # write result to STDOUT
            for each in arrayResult:
                # print("okkkkkkkkkkkkkkkkkkkkk")
                current_count += each + ', ' + joinKey + '\n'
            print '%s' % (current_count)
        
        elif current_word != -1 or joinKey:
            pass

        arrayResult = []
        current_count = ''
        if type == 'movie':
            joinKey = count
        else:
            joinKey = None
            arrayResult.append(count)
        # current_count = count
        current_word = int(word)
        
        

# # do not forget to output the last word if needed!
# print(current_word, word, joinKey, arrayResult, type)
if len(arrayResult) > 0 and joinKey:
    for each in arrayResult:
        # print("okkkkkkkkkkkkkkkkkkkkk")
        current_count += each + ', ' + joinKey + '\n'
    print '%s' % (current_count)
elif len(arrayResult) == 0 and joinKey:
    print '%s' % (current_count)